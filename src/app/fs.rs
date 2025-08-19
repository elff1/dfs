use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::{select, sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    app::{ServerError, Service, p2p::FileChunkId},
    file_store,
};

mod helper;
mod processor;
mod worker;
pub use helper::*;
pub use processor::*;
use worker::*;

const LOG_TARGET: &str = "app::fs";
const FS_CHUNK_WORKER_NUM: usize = 8;
const PROCESSING_RESULT_FILE_NAME: &str = "metadata.cbor";

#[derive(Debug, Error)]
pub enum FsServiceError {
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
}

pub enum FsCommand {
    ReadMetadata {
        file_id: u64,
        tx: mpsc::Sender<(FileChunkId, Option<Vec<u8>>)>,
    },
    // WriteMetadata {
    //     file_id: u64,
    //     chunks_directory: PathBuf,
    //     contents: Vec<u8>,
    // },
}

pub enum FsChunkCommand {
    Read {
        chunk_id: FileChunkId,
        tx: mpsc::Sender<(FileChunkId, Option<Vec<u8>>)>,
    },
    // Write {
    //     chunk_id: FileChunkId,
    //     chunks_directory: PathBuf,
    //     contents: Vec<u8>,
    // },
}

#[derive(Debug)]
pub struct FsService<F: file_store::Store + Send + Sync + 'static> {
    file_store: Arc<F>,
    fs_command_rx: mpsc::Receiver<FsCommand>,
    fs_chunk_command_rx: async_channel::Receiver<FsChunkCommand>,
}

impl<F: file_store::Store + Send + Sync + 'static> FsService<F> {
    pub fn new(
        file_store: Arc<F>,
        fs_command_rx: mpsc::Receiver<FsCommand>,
        fs_chunk_command_rx: async_channel::Receiver<FsChunkCommand>,
    ) -> Self {
        Self {
            file_store,
            fs_command_rx,
            fs_chunk_command_rx,
        }
    }

    async fn handle_command_read_metadata(
        &self,
        file_id: u64,
        tx: mpsc::Sender<(FileChunkId, Option<Vec<u8>>)>,
    ) -> Option<()> {
        let chunks_directory = self
            .file_store
            .get_published_file_chunks_directory(file_id)
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Get file store record of file[{file_id}] failed: {e}");
            })
            .ok()?;

        let metadata = FsHelper::read_file_metadata(&chunks_directory)
            .await
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Read metadata of file[{file_id}] at [{}] failed: {e}",
                    chunks_directory.display());
            })
            .ok();

        tx.send((FileChunkId::new(file_id, 0), metadata))
            .await
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Send metadata of file[{file_id}] failed: {e}");
            })
            .ok()
    }

    // pub async fn write_file_metadata<P: AsRef<Path>>(
    //     chunks_directory: P,
    //     metadata: &FileProcessResult,
    // ) -> io::Result<()> {
    //     let cbor_file =
    //         fs::File::create(chunks_directory.as_ref().join(PROCESSING_RESULT_FILE_NAME))
    //             .await?
    //             .into_std()
    //             .await;
    //     serde_cbor::to_writer(cbor_file, metadata).map_err(|e| io::Error::other(e.to_string()))?;

    //     Ok(())
    // }

    async fn handle_command(&mut self, command: FsCommand) -> Option<()> {
        match command {
            FsCommand::ReadMetadata { file_id, tx } => {
                self.handle_command_read_metadata(file_id, tx).await?;
            }
        }

        Some(())
    }

    async fn start_inner(mut self, cancel_token: CancellationToken) -> Result<(), FsServiceError> {
        loop {
            select! {
                command = self.fs_command_rx.recv() => {
                    if let Some(command) = command {
                        self.handle_command(command).await;
                    }
                }
                _ = cancel_token.cancelled() => {
                    log::info!(target: LOG_TARGET, "FS service is shutting down...");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<F: file_store::Store + Send + Sync + 'static> Service for FsService<F> {
    async fn start(mut self, cancel_token: CancellationToken) -> Result<(), ServerError> {
        log::info!(target: LOG_TARGET, "FS service starting...");

        let mut worker_set = JoinSet::new();

        for i in 0..FS_CHUNK_WORKER_NUM {
            let worker = FsServiceWorker {
                worker_id: i,
                cancel_token: cancel_token.clone(),
                file_store: self.file_store.clone(),
                fs_chunk_command_rx: self.fs_chunk_command_rx.clone(),
            };

            worker_set.spawn_blocking(async move || worker.start().await);
        }

        self.start_inner(cancel_token).await?;

        worker_set.join_all().await;

        log::info!(target: LOG_TARGET, "FS service has shut down");

        Ok(())
    }
}
