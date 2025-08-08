use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use thiserror::Error;
use tokio::{fs, io, select, sync::mpsc};
use tokio_util::sync::CancellationToken;

use super::{ServerError, Service};
use crate::{
    file_processor::{FileProcessResult, FileProcessResultHash, FileProcessor},
    file_store::{self, DownloadingFileRecord, FileStoreError},
};

const LOG_TARGET: &str = "app::download";

#[derive(Debug, Error)]
pub enum DownloadServicekError {
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
    ) -> Result<(), DownloadServicekError> {
        download_path.push(hex::encode(id.to_array()));
        let _ = fs::remove_dir_all(&download_path).await;
        fs::create_dir_all(&download_path).await?;

        FileProcessor::write_file_metadata(&download_path, &metadata).await?;

        self.file_store
            .add_downloading_file(DownloadingFileRecord {
                id,
                original_file_name: metadata.original_file_name,
                download_directory: download_path,
            })?;

        /* TODO:
          Updata metadata download:
          1. oneshoot send/recv vec<u8>
          2. P2pCommand::download request, file_id, chunk_id(0: metadata, 1..n: chunks)
          3. updata DHT record key: publish, get
          4. handle download request event: read db, read fs, send event response

          file download:
          1. spwan a task to download chunks
          2. publish chunks on P2P DHT
          3. send command to P2P, download a chunk from peer like metadata (How to download parallel)
          4. after all chunks downloaded, merge together
          5. send command to P2P, publish file
          6. move file id from Downloading table to pubished table

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
    ) -> Result<(), DownloadServicekError> {
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
        log::debug!(target: LOG_TARGET, "DownloadService starting...");

        self.start_inner(cancel_token).await?;

        Ok(())
    }
}
