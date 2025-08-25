use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use thiserror::Error;
use tokio::{io, select, sync::mpsc};
use tokio_util::sync::CancellationToken;

use super::{
    ServerError, Service,
    fs::{FileProcessResult, FileProcessResultHash, FsHelper},
};
use crate::file_store::{self, DownloadingFileRecord, FileStoreError};

const LOG_TARGET: &str = "app::download";

#[derive(Debug, Error)]
pub enum DownloadServiceError {
    #[error("I/O error: {0}")]
    IO(#[from] io::Error),
    #[error("File store error: {0}")]
    FileStore(#[from] FileStoreError),
}

pub enum DownloadCommand {
    OneFile {
        id: FileProcessResultHash,
        metadata: Box<FileProcessResult>,
        download_path: PathBuf,
    },
}

#[derive(Debug)]
pub struct DownloadService<F: file_store::Store + Send + Sync + 'static> {
    file_store: Arc<F>,
    download_command_rx: mpsc::Receiver<DownloadCommand>,
}

impl<F: file_store::Store + Send + Sync + 'static> DownloadService<F> {
    pub fn new(file_store: Arc<F>, download_command_rx: mpsc::Receiver<DownloadCommand>) -> Self {
        Self {
            file_store,
            download_command_rx,
        }
    }

    async fn handle_download_one_file(
        &mut self,
        id: FileProcessResultHash,
        metadata: Box<FileProcessResult>,
        mut download_path: PathBuf,
    ) -> Result<(), DownloadServiceError> {
        download_path.push(hex::encode(id.to_array()));

        FsHelper::create_directory_async(&download_path, true).await?;

        // let (tx, rx) = oneshot::channel();
        // self.p2p_command_tx
        //     .send(P2pCommand::DownloadFile {
        //         id: FileChunkId::new(file_id, 0),
        //         tx,
        //     })
        //     .await
        //     .map_err(|e| format!("Send metadata request to P2P service failed: {e}"))?;

        // let metadata: Box<FileProcessResult> = rx
        //     .await
        //     .map_err(|e| format!("Receive metadata from P2P service failed: {e}"))?
        //     .ok_or("Download [None] metadata")?
        //     .as_slice()
        //     .try_into()
        //     .map_err(|e| format!("Parse metadata failed: {e}"))?;

        // log::info!(target: LOG_TARGET, "Downloaded metadata of file[{file_id} | {}] with {} chunks",
        //     metadata.original_file_name, metadata.number_of_chunks);

        FsHelper::write_file_metadata(&download_path, &metadata.merkle_root)?;

        self.file_store
            .add_downloading_file(DownloadingFileRecord {
                id,
                original_file_name: metadata.original_file_name,
                download_directory: download_path,
            })?;

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
          5. after all chunks downloaded, merge together
          6. send command to P2P, publish file
          7. move file id from Downloading table to pubished table

          remove file from dfs system
          1. remove record from DHT
          2. remove record from DB
          3. remove chunks from fs
          4. remove file from fs if requred
        */

        Ok(())
    }

    async fn handle_command(&mut self, command: DownloadCommand) -> Option<()> {
        match command {
            DownloadCommand::OneFile {
                id,
                metadata,
                download_path,
            } => {
                self.handle_download_one_file(id, metadata, download_path)
                    .await
                    .map_err(|e| {
                        log::error!(target: LOG_TARGET, "Start download file[{}] failed: {e}", id.raw());
                    })
                    .ok()?;
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
