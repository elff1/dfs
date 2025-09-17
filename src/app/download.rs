use std::{
    collections::BinaryHeap,
    ops::Not,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
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
    fs::{FsCommand, FsHelper},
    p2p::P2pCommand,
};
use crate::{
    FileChunkId, FileId, FileMetadata,
    file_store::{self, DownloadingFileRecord},
};

const LOG_TARGET: &str = "app::download";
const DOWNLOAD_CHUNK_RETRY_TIMES: u8 = 2;

#[derive(Debug, Error)]
pub enum DownloadServiceError {
    #[error("I/O error: {0}")]
    IO(#[from] io::Error),
    // #[error("File store error: {0}")]
    // FileStore(#[from] FileStoreError),
}

#[derive(Clone, Copy)]
struct DownloadChunkTask {
    chunk_id: FileChunkId,
    retry_times: u8,
}

impl PartialEq for DownloadChunkTask {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for DownloadChunkTask {}

impl PartialOrd for DownloadChunkTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DownloadChunkTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.retry_times.cmp(&other.retry_times) {
            std::cmp::Ordering::Equal => other.chunk_id.chunk_index.cmp(&self.chunk_id.chunk_index),
            ord => ord,
        }
    }
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
    fs_command_tx: mpsc::Sender<FsCommand>,
    p2p_command_tx: mpsc::Sender<P2pCommand>,
    download_command_rx: mpsc::Receiver<DownloadCommand>,
}

impl<F: file_store::Store + Send + Sync + 'static> DownloadService<F> {
    pub fn new(
        file_store: Arc<F>,
        fs_command_tx: mpsc::Sender<FsCommand>,
        p2p_command_tx: mpsc::Sender<P2pCommand>,
        download_command_rx: mpsc::Receiver<DownloadCommand>,
    ) -> Self {
        Self {
            file_store,
            fs_command_tx,
            p2p_command_tx,
            download_command_rx,
        }
    }

    async fn download_metadata(
        &self,
        file_id: FileId,
        download_directory: &Path,
    ) -> Option<Box<FileMetadata>> {
        let read_metadata_result = FsHelper::read_file_metadata_async(download_directory)
            .await
            .ok()
            .and_then(|metadata| {
                <Box<FileMetadata>>::try_from(metadata.as_slice()).map_err(|e| {
                    log::error!(target: LOG_TARGET, "Parse file[{file_id}] metadata failed: {e}");
                })
                .ok()
            });
        if read_metadata_result.is_some() {
            log::info!(target: LOG_TARGET, "Metadata of file[{file_id}] is already downloaded");
            return read_metadata_result;
        }

        let (tx, rx) = oneshot::channel();
        self.p2p_command_tx
            .send(P2pCommand::DownloadFile {
                chunk_id: FileChunkId::new(file_id, 0),
                chunk_hash: Default::default(),
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

        let metadata = <Box<FileMetadata>>::try_from(metadata_contents.as_slice())
            .map_err(|e| log::error!(target: LOG_TARGET, "Parse metadata[{file_id}] failed: {e}"))
            .ok()
            .filter(|metadata| {
                metadata.verify() || {
                    log::error!(target: LOG_TARGET, "Verify metadata[{file_id}] failed");
                    false
                }
            })?;

        log::info!(target: LOG_TARGET, "Downloaded metadata of file[{file_id}][{}] with {} chunks",
            metadata.original_file_name, metadata.number_of_chunks);

        FsHelper::write_file_metadata_async(download_directory, metadata_contents)
            .await
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Fs write metadata[{file_id}] failed: {e}");
            })
            .ok()?;

        Some(metadata)
    }

    async fn download_chunks(
        &self,
        mut download_chunk_tasks: BinaryHeap<DownloadChunkTask>,
        download_directory: &Path,
        metadata: &FileMetadata,
    ) -> BinaryHeap<DownloadChunkTask> {
        let mut download_set = JoinSet::new();

        loop {
            select! {
                Some((download_chunk_task, download_chunk_rx)) = async {
                    let download_chunk_task = download_chunk_tasks.peek()?;
                    if download_chunk_task.retry_times == 0 {
                        return None;
                    }

                    let (tx, rx) = oneshot::channel();
                    let send_command_result = self
                        .p2p_command_tx
                        .send(P2pCommand::DownloadFile {
                            chunk_id: download_chunk_task.chunk_id,
                            chunk_hash: metadata.chunk_hashes[download_chunk_task.chunk_id.chunk_index - 1],
                            downloaded_contents_tx: tx,
                        })
                        .await;

                    // pop after await, make sure this branch is cancellation safe
                    let mut download_chunk_task = download_chunk_tasks.pop().unwrap();
                    match send_command_result {
                        Err(e) => {
                            log::error!(target: LOG_TARGET, "Send download chunk[{}] to P2P service failed: {e}", download_chunk_task.chunk_id);

                            download_chunk_task.retry_times -= 1;
                            download_chunk_tasks.push(download_chunk_task);

                            None
                        }
                        Ok(_) => {
                            log::debug!(target: LOG_TARGET, "Start download chunk[{}]", download_chunk_task.chunk_id);
                            Some((download_chunk_task, rx))
                        }
                    }
                } => {
                    let chunk_merkle_hash = metadata.merkle_leaves[download_chunk_task.chunk_id.chunk_index - 1];
                    let download_directory = download_directory.to_path_buf();

                    download_set.spawn(async move {
                        let chunk = match download_chunk_rx.await {
                            Err(e) => {
                                log::error!(target: LOG_TARGET, "Receive chunk[{}] from P2P service failed: {e}", download_chunk_task.chunk_id);
                                None
                            }
                            Ok(None) => {
                                log::error!(target: LOG_TARGET, "Receive chunk[{}]: None", download_chunk_task.chunk_id);
                                None
                            }
                            Ok(chunk) => chunk,
                        }.ok_or(download_chunk_task)?;

                        if Sha256::hash(chunk.as_slice()) != chunk_merkle_hash {
                            log::error!(target: LOG_TARGET, "Check chunk[{}] hash failed", download_chunk_task.chunk_id);
                            return Err(download_chunk_task);
                        }

                        FsHelper::write_file_chunk_async(&download_directory, download_chunk_task.chunk_id.chunk_index, chunk)
                            .await
                            .map_err(|e| {
                                log::error!(target: LOG_TARGET, "Write chunk[{}] to fs failed: {e}", download_chunk_task.chunk_id);
                                download_chunk_task
                            })?;

                        Ok(download_chunk_task.chunk_id)
                    });
                }
                Some(download_result) = download_set.join_next() => {
                    match download_result.unwrap() {
                        Ok(chunk_id) => {
                            log::info!(target: LOG_TARGET, "Chunk[{chunk_id}] downloaded");
                        }
                        Err(mut download_chunk_task) => {
                            log::error!(target: LOG_TARGET, "Download chunk[{}] failed. Try again later.", download_chunk_task.chunk_id);

                            download_chunk_task.retry_times -= 1;
                            download_chunk_tasks.push(download_chunk_task);
                        }
                    };
                }
                else => break,
            }
        }

        download_chunk_tasks
    }

    // Only new file download and restore unfinished file download run into this function
    async fn download_one_file(&self, file_id: FileId, download_directory: PathBuf) -> Option<()> {
        log::info!(target: LOG_TARGET, "Start to download file[{file_id}]");

        let mut try_again = true;
        let metadata = loop {
            if let Some(metadata_result) =
                self.download_metadata(file_id, &download_directory).await
            {
                break metadata_result;
            }

            if try_again {
                try_again = false;
            } else {
                return None;
            }
        };

        let number_of_chunks = metadata.number_of_chunks as usize;
        let mut need_download_chunk_tasks = BinaryHeap::with_capacity(number_of_chunks);
        for chunk_index in 1..=number_of_chunks {
            let chunk_id = FileChunkId::new(file_id, chunk_index);

            let need_download = if let Ok(chunk) =
                FsHelper::read_file_chunk_async(&download_directory, chunk_index).await
            {
                Sha256::hash(chunk.as_slice()) != metadata.merkle_leaves[chunk_index - 1]
            } else {
                true
            };

            if need_download {
                need_download_chunk_tasks.push(DownloadChunkTask {
                    chunk_id,
                    retry_times: DOWNLOAD_CHUNK_RETRY_TIMES,
                });
            } else {
                log::debug!(target: LOG_TARGET, "Chunk[{chunk_id}] is already downloaded");
            }
        }

        let download_failed_chunk_tasks = self
            .download_chunks(need_download_chunk_tasks, &download_directory, &metadata)
            .await;
        if download_failed_chunk_tasks.is_empty().not() {
            log::error!(target: LOG_TARGET, "{} of {} chunks download failed", download_failed_chunk_tasks.len(), metadata.number_of_chunks);
            return None;
        }

        let (tx, rx) = oneshot::channel();
        self.fs_command_tx
            .send(FsCommand::ProcessFileAfterDownload {
                chunks_directory: download_directory,
                file_id,
                original_file_name: metadata.original_file_name.clone(),
                number_of_chunks: metadata.number_of_chunks,
                public: metadata.public,
                process_result_tx: tx,
            })
            .await
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Send command to FS service failed: {e}");
            })
            .ok()?;

        match rx.await {
            Err(e) => {
                log::error!(target: LOG_TARGET, "Receive response for file[{file_id}] from FS service failed: {e}");
                None
            }
            Ok(false) => {
                log::error!(target: LOG_TARGET, "Process file[{file_id}] after download failed");
                None
            }
            Ok(_) => {
                log::info!(target: LOG_TARGET, "File[{file_id}] downloaded");
                Some(())
            }
        }?;

        self.file_store.delete_downloading_file(file_id).unwrap_or_else(|e| {
                log::error!(target: LOG_TARGET, "Delete downloading file from file store failed: {e}")
            });

        self.p2p_command_tx
            .send(P2pCommand::PublishFile(metadata))
            .await
            .unwrap_or_else(|e| {
                log::error!(target: LOG_TARGET,
                    "Send publish file[{file_id}] request to P2P service failed: {e}");
            });

        /* TODO:
          remove file from dfs system
          1. remove record from DHT
          2. remove record from DB
          3. remove chunks from fs
          4. remove file from fs if requred

          1. support download folder
          2. download pause/resume; restart failed file download; remove published file if deleted or changed
          3. parallel files download
          4. restart download after 2s; download speed
        */

        Some(())
    }

    async fn handle_command_download_one_file(
        &self,
        file_id: FileId,
        mut download_path: PathBuf,
    ) -> Option<()> {
        // if file is downloaded or downloading, no need to download again
        let mut file_exists = self.file_store.published_file_exists(file_id).map_err(|e| {
                log::error!(target: LOG_TARGET, "Check whether file[{file_id}] is published failed: {e}");
            })
            .into_iter()
            .chain(self.file_store.downloading_file_exists(file_id).map_err(|e| {
                log::error!(target: LOG_TARGET, "Check whether file[{file_id}] is downloading failed: {e}");
            }));

        let file_exists = match file_exists.next() {
            Some(false) => file_exists.next(),
            Some(true) => Some(true),
            None => None,
        };

        if file_exists != Some(false) {
            if file_exists.is_some() {
                log::warn!(target: LOG_TARGET, "No need to download file[{file_id}]");
            }

            return None;
        };

        // start to download
        download_path.push(file_id.to_string());

        FsHelper::create_directory_async(&download_path, true)
            .await
            .map_err(|e| {
                log::error!(target: LOG_TARGET,
                    "Create download directory[{}] failed: {e}", download_path.display());
            })
            .ok()?;

        self.file_store
            .add_downloading_file(DownloadingFileRecord {
                file_id,
                download_directory: download_path.clone(),
            })
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Record downloading file[{file_id}] to file store failed: {e}");
            })
            .ok()?;

        self.download_one_file(file_id, download_path).await
    }

    async fn handle_command(&self, command: DownloadCommand) -> Option<()> {
        match command {
            DownloadCommand::OneFile {
                file_id,
                download_path,
            } => {
                self.handle_command_download_one_file(file_id, download_path)
                    .await?;
            }
        }

        Some(())
    }

    async fn download_unfinished_files(&self) -> Option<()> {
        let records = self.file_store.get_all_downloading_files().map_err(|e| {
            log::error!(target: LOG_TARGET, "Get all downloadinig files from file store failed: {e}");
        }).ok()?;

        for DownloadingFileRecord {
            file_id,
            download_directory,
        } in records
        {
            FsHelper::create_directory_async(&download_directory, false)
                .await
                .map_err(|e| {
                    log::error!(target: LOG_TARGET,
                        "Create download directory[{}] failed: {e}", download_directory.display()
                    );
                })
                .ok()?;

            self.download_one_file(file_id, download_directory).await;
        }

        Some(())
    }

    async fn start_inner(
        mut self,
        cancel_token: CancellationToken,
    ) -> Result<(), DownloadServiceError> {
        // wait for P2P network ready
        tokio::time::sleep(Duration::from_secs(3)).await;

        self.download_unfinished_files().await;

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

        log::info!(target: LOG_TARGET, "Download service has shut down");

        Ok(())
    }
}
