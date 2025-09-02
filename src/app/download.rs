use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use rs_merkle::{Hasher, algorithms::Sha256};
use thiserror::Error;
use tokio::{
    io, select,
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use super::{
    ServerError, Service,
    fs::{FileProcessResult, FsHelper},
    p2p::P2pCommand,
};
use crate::{
    FileChunkId, FileId,
    file_store::{self, DownloadingFileRecord},
};

const LOG_TARGET: &str = "app::download";

#[derive(Debug, Error)]
pub enum DownloadServiceError {
    #[error("I/O error: {0}")]
    IO(#[from] io::Error),
    // #[error("File store error: {0}")]
    // FileStore(#[from] FileStoreError),
}

pub enum DownloadCommand {
    OneFile {
        file_id: FileId,
        download_path: PathBuf,
    },
}

#[derive(Debug)]
pub struct DownloadService<F: file_store::Store + Send + Sync + 'static> {
    file_store: Arc<F>,
    p2p_command_tx: mpsc::Sender<P2pCommand>,
    download_command_rx: mpsc::Receiver<DownloadCommand>,
}

impl<F: file_store::Store + Send + Sync + 'static> DownloadService<F> {
    pub fn new(
        file_store: Arc<F>,
        p2p_command_tx: mpsc::Sender<P2pCommand>,
        download_command_rx: mpsc::Receiver<DownloadCommand>,
    ) -> Self {
        Self {
            file_store,
            p2p_command_tx,
            download_command_rx,
        }
    }

    async fn download_metadata(
        &mut self,
        file_id: FileId,
        download_directory: &Path,
    ) -> Option<Box<FileProcessResult>> {
        FsHelper::create_directory_async(download_directory, true)
            .await
            .map_err(|e| {
                log::error!(target: LOG_TARGET,
                    "Create download directory[{}] failed: {e}", download_directory.display());
            })
            .ok()?;

        let (tx, rx) = oneshot::channel();
        self.p2p_command_tx
            .send(P2pCommand::DownloadFile {
                chunk_id: FileChunkId::new(file_id, 0),
                downloaded_contents_tx: tx,
            })
            .await
            .map_err(|e| {
                log::error!(target: LOG_TARGET,
                    "Send download metadata[{file_id}] request to P2P service failed: {e}");
            })
            .ok()?;

        let metadata_contents = rx
            .await
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Receive metadata[{file_id}] from P2P service failed: {e}");
            }).ok()?
            .or_else(|| {
                log::error!(target: LOG_TARGET, "Receive metadata[{file_id}]: None");
                None
            })?;

        let metadata: Box<FileProcessResult> = metadata_contents
            .as_slice()
            .try_into()
            .map_err(|e| log::error!(target: LOG_TARGET, "Parse metadata[{file_id}] failed: {e}"))
            .ok()?;

        log::info!(target: LOG_TARGET, "Downloaded metadata of file[{file_id}][{}] with {} chunks",
            metadata.original_file_name, metadata.number_of_chunks);

        // TODO: verify metadata

        FsHelper::write_file_metadata(download_directory, metadata_contents)
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Fs write metadata[{file_id}] failed: {e}");
            })
            .ok()?;

        self.file_store
            .add_downloading_file(DownloadingFileRecord {
                file_id,
                original_file_name: metadata.original_file_name.clone(),
                download_directory: download_directory.to_path_buf(),
            })
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Record downloading file[{file_id}] to file store failed: {e}");
            })
            .ok()?;

        Some(metadata)
    }

    async fn download_chunks(
        &mut self,
        file_id: FileId,
        download_directory: &Path,
        metadata: &FileProcessResult,
    ) -> u32 {
        let mut chunk_ids = (1..=metadata.number_of_chunks as usize)
            .map(|chunk_index| FileChunkId::new(file_id, chunk_index))
            .collect::<VecDeque<_>>();
        let mut download_set = JoinSet::new();
        let mut downloaded_chunks_num = 0;

        loop {
            select! {
                Some((chunk_id, download_chunk_rx)) = async {
                    let chunk_id = chunk_ids.front()?;

                    let (tx, rx) = oneshot::channel();
                    let send_command_result = self
                        .p2p_command_tx
                        .send(P2pCommand::DownloadFile {
                            chunk_id: *chunk_id,
                            downloaded_contents_tx: tx,
                        })
                        .await;

                    // pop after await, make sure this branch is cancellation safe
                    let chunk_id = chunk_ids.pop_front().unwrap();
                    match send_command_result {
                        Ok(_) => {
                            log::debug!(target: LOG_TARGET, "Start download chunk[{chunk_id}]");
                            Some((chunk_id, rx))
                        }
                        Err(e) => {
                            log::error!(target: LOG_TARGET, "Send download chunk[{chunk_id}] to P2P service failed: {e}");
                            None
                        }
                    }
                } => {
                    let chunk_hash = metadata.merkle_leaves[chunk_id.chunk_index];
                    let download_directory = download_directory.to_path_buf();

                    download_set.spawn(async move {
                        let chunk = match download_chunk_rx.await {
                            Err(e) => {
                                log::error!(target: LOG_TARGET, "Receive chunk[{chunk_id}] from P2P service failed: {e}");
                                None
                            }
                            Ok(None) => {
                                log::error!(target: LOG_TARGET, "Receive chunk[{chunk_id}]: None");
                                None
                            }
                            Ok(chunk) => chunk,
                        }.ok_or(chunk_id)?;

                        if Sha256::hash(&chunk) != chunk_hash {
                            log::error!(target: LOG_TARGET, "Check chunk[{chunk_id}] hash failed");
                            return Err(chunk_id);
                        }

                        FsHelper::write_file_chunk(&download_directory, chunk_id.chunk_index, chunk)
                            .map_err(|e| {
                                log::error!(target: LOG_TARGET, "Write chunk[{chunk_id}] to fs failed: {e}");
                                chunk_id
                            })?;

                        Ok(chunk_id)
                    });
                }
                Some(download_result) = download_set.join_next() => {
                    match download_result.unwrap() {
                        Ok(chunk_id) => {
                            log::info!(target: LOG_TARGET, "Chunk[{chunk_id}] downloaded");
                            downloaded_chunks_num += 1;
                        }
                        Err(chunk_id) => {
                            log::error!(target: LOG_TARGET, "Download chunk[{chunk_id}] failed. Try again later.");
                            chunk_ids.push_back(chunk_id);
                        }
                    };
                }
                else => break,
            }
        }

        downloaded_chunks_num
    }

    async fn handle_download_one_file(
        &mut self,
        file_id: FileId,
        mut download_path: PathBuf,
    ) -> Option<()> {
        download_path.push(hex::encode(file_id.to_array()));

        let metadata = self.download_metadata(file_id, &download_path).await?;

        let downloaded_chunks_num = self
            .download_chunks(file_id, &download_path, &metadata)
            .await;
        if downloaded_chunks_num != metadata.number_of_chunks {
            log::error!(target: LOG_TARGET, "Only {downloaded_chunks_num} of {} chunks downloaded", metadata.number_of_chunks);
            return None;
        }

        // TODO: merge chunks

        self.p2p_command_tx
            .send(P2pCommand::PublishFile(metadata))
            .await
            .unwrap_or_else(|e| {
                log::error!(target: LOG_TARGET,
                    "Send publish file[{file_id}] request to P2P service failed: {e}");
            });

        self.file_store.delete_downloading_file(file_id).unwrap_or_else(|e| {
                log::error!(target: LOG_TARGET, "Delete downloading file from file store failed: {e}")
            });

        /* TODO:
              make file processor a multi-thread service, each thread wait on a mpmc rx(using sync fs call). commands:
              1. process file (split into chunks)
              2. read chunk

              publish file:
              do not delete the folder

              metadata download:
              1. oneshoot send/recv vec<u8>
              2. P2pCommand::download request, file_id, chunk_id(0: metadata, 1..n: chunks)
              3. updata DHT record key: publish, get
              4. handle download request event: read DB, read fs(file processor is multi-thread), send event response(swam is multi-thread)

              file download (one file at a time):
              1. publish chunks on P2P DHT
              2. delete download folder
              3. download metadata, write to fs(create folder automatically if not exist), record to DB Downloading table
              4. send download chunk task to task pool submodule
                a. send command to P2P, download a chunk from peer like metadata
                b. check hash, write chunk to fs
        --->  5. after all chunks downloaded, merge together
              6. send command to P2P, publish file
              7. move file id from Downloading table to pubished table
              8. continue unfinished download work after restart

              remove file from dfs system
              1. remove record from DHT
              2. remove record from DB
              3. remove chunks from fs
              4. remove file from fs if requred

              1. fs: adjust metadata(add FileMetadata): remove chunks_directory, add file length
              2. p2p: add Hash256; add DhtKey(u64): metadata = FileId, chunk = hash(Sha256)
              3. support download folder
            */

        Some(())
    }

    async fn handle_command(&mut self, command: DownloadCommand) -> Option<()> {
        match command {
            DownloadCommand::OneFile {
                file_id,
                download_path,
            } => {
                self.handle_download_one_file(file_id, download_path)
                    .await?;
            }
        }

        Some(())
    }

    async fn start_inner(
        mut self,
        cancel_token: CancellationToken,
    ) -> Result<(), DownloadServiceError> {
        loop {
            select! {
                command = self.download_command_rx.recv() => {
                    if let Some(command) = command {
                        self.handle_command(command).await;
                    }
                }
                _ = cancel_token.cancelled() => {
                    log::info!(target: LOG_TARGET, "Download service is shutting down...");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<F: file_store::Store + Send + Sync + 'static> Service for DownloadService<F> {
    async fn start(self, cancel_token: CancellationToken) -> Result<(), ServerError> {
        log::info!(target: LOG_TARGET, "Download service starting...");

        self.start_inner(cancel_token).await?;

        Ok(())
    }
}
