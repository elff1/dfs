use std::{io, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use thiserror::Error;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{
    FileChunkId, FileId, FileMetadata,
    app::{ServerError, Service},
    file_store,
};

mod helper;
mod processor;
mod worker;
pub use helper::*;
use processor::*;
use worker::*;

const LOG_TARGET: &str = "app::fs";
const FS_CHUNK_WORKER_NUM: usize = 8;

#[derive(Debug, Error)]
pub enum FsServiceError {
    #[error("I/O error: {0}")]
    IO(#[from] io::Error),
}

pub enum FsCommand {
    ProcessFileBeforePublish {
        file_path: String,
        public: bool,
        metadata_tx: oneshot::Sender<Option<Box<FileMetadata>>>,
    },
    ProcessFileAfterDownload {
        chunks_directory: PathBuf,
        file_id: FileId,
        original_file_name: String,
        number_of_chunks: u32,
        public: bool,
        process_result_tx: oneshot::Sender<bool>,
    },
    ReadMetadata {
        file_id: FileId,
        contents_tx: mpsc::Sender<(FileChunkId, Option<Vec<u8>>)>,
    },
    ReadChunk {
        chunk_id: FileChunkId,
        contents_tx: mpsc::Sender<(FileChunkId, Option<Vec<u8>>)>,
    },
}

enum FileProcessCommand {
    BeforePublish {
        file_path: String,
        public: bool,
        metadata_tx: Option<oneshot::Sender<Option<Box<FileMetadata>>>>,
    },
    AfterDownload {
        chunks_directory: Option<PathBuf>,
        file_id: FileId,
        original_file_name: Option<String>,
        number_of_chunks: u32,
        public: bool,
        process_result_tx: Option<oneshot::Sender<bool>>,
    },
}

enum FileMetadataCommand {
    Read {
        file_id: FileId,
        contents_tx: Option<mpsc::Sender<(FileChunkId, Option<Vec<u8>>)>>,
    },
}

enum FileChunkCommand {
    Read {
        chunk_id: FileChunkId,
        contents_tx: Option<mpsc::Sender<(FileChunkId, Option<Vec<u8>>)>>,
    },
}

#[derive(Debug)]
pub struct FsService<F: file_store::Store + Send + Sync + 'static> {
    file_store: Arc<F>,
    fs_command_rx: mpsc::Receiver<FsCommand>,
    file_process_command_tx: async_channel::Sender<FileProcessCommand>,
    file_process_command_rx: async_channel::Receiver<FileProcessCommand>,
    file_metadata_command_tx: async_channel::Sender<FileMetadataCommand>,
    file_metadata_command_rx: async_channel::Receiver<FileMetadataCommand>,
    file_chunk_command_tx: async_channel::Sender<FileChunkCommand>,
    file_chunk_command_rx: async_channel::Receiver<FileChunkCommand>,
}

impl<F: file_store::Store + Send + Sync + 'static> FsService<F> {
    pub fn new(file_store: Arc<F>, fs_command_rx: mpsc::Receiver<FsCommand>) -> Self {
        let (file_process_command_tx, file_process_command_rx) =
            async_channel::bounded::<FileProcessCommand>(100);
        let (file_metadata_command_tx, file_metadata_command_rx) =
            async_channel::bounded::<FileMetadataCommand>(100);
        let (file_chunk_command_tx, file_chunk_command_rx) =
            async_channel::bounded::<FileChunkCommand>(100);

        Self {
            file_store,
            fs_command_rx,
            file_process_command_tx,
            file_process_command_rx,
            file_metadata_command_tx,
            file_metadata_command_rx,
            file_chunk_command_tx,
            file_chunk_command_rx,
        }
    }

    async fn handle_command(&mut self, command: FsCommand) -> Option<()> {
        match command {
            FsCommand::ProcessFileBeforePublish {
                file_path,
                public,
                metadata_tx,
            } => {
                self.file_process_command_tx
                    .try_send(FileProcessCommand::BeforePublish {
                        file_path,
                        public,
                        metadata_tx: Some(metadata_tx),
                    })
                    .map_err(|e| {
                        log::error!(target: LOG_TARGET, "Distribute process before publish file to worker failed: {e}")
                    })
                    .ok()?;
            }
            FsCommand::ProcessFileAfterDownload {
                chunks_directory,
                file_id,
                original_file_name,
                number_of_chunks,
                public,
                process_result_tx,
            } => {
                self.file_process_command_tx
                    .try_send(FileProcessCommand::AfterDownload {
                        chunks_directory: Some(chunks_directory),
                        file_id,
                        original_file_name: Some(original_file_name),
                        number_of_chunks,
                        public,
                        process_result_tx: Some(process_result_tx),
                    })
                    .map_err(|e| {
                        log::error!(target: LOG_TARGET, "Distribute process after download file[{file_id}] to worker failed: {e}")
                    })
                    .ok()?;
            }
            FsCommand::ReadMetadata {
                file_id,
                contents_tx,
            } => {
                self.file_metadata_command_tx
                    .try_send(FileMetadataCommand::Read{ file_id, contents_tx: Some(contents_tx) })
                    .map_err(|e| {
                        log::error!(target: LOG_TARGET, "Distribute read metadata of file[{file_id}] to worker failed: {e}")
                    })
                    .ok()?;
            }
            FsCommand::ReadChunk {
                chunk_id,
                contents_tx,
            } => {
                self.file_chunk_command_tx
                    .try_send(FileChunkCommand::Read{ chunk_id, contents_tx: Some(contents_tx) })
                    .map_err(|e| {
                        log::error!(target: LOG_TARGET, "Distribute read chunk[{chunk_id}] to worker failed: {e}")
                    })
                    .ok()?;
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
                file_process_command_rx: self.file_process_command_rx.clone(),
                file_metadata_command_rx: self.file_metadata_command_rx.clone(),
                file_chunk_command_rx: self.file_chunk_command_rx.clone(),
            };

            worker_set.spawn(worker.start());
        }

        self.start_inner(cancel_token).await?;

        worker_set.join_all().await;

        log::info!(target: LOG_TARGET, "FS service has shut down");

        Ok(())
    }
}

impl Drop for FileProcessCommand {
    fn drop(&mut self) {
        match self {
            FileProcessCommand::BeforePublish {
                file_path,
                public: _,
                metadata_tx,
            } => {
                if let Some(tx) = metadata_tx.take() {
                    log::warn!(target: LOG_TARGET,
                        "Send None as response of process before publish file[{file_path}] because of failure"
                    );

                    tx.send(None).unwrap_or_else(|_| {
                        log::error!(target: LOG_TARGET,
                            "Send response of process before publish file[{file_path}] failed"
                        );
                    });
                }
            }
            FileProcessCommand::AfterDownload {
                chunks_directory: _,
                file_id,
                original_file_name: _,
                number_of_chunks: _,
                public: _,
                process_result_tx,
            } => {
                if let Some(tx) = process_result_tx.take() {
                    log::warn!(target: LOG_TARGET,
                        "Send false as response of process after download file[{file_id}] because of failure"
                    );

                    tx.send(false).unwrap_or_else(|_| {
                        log::error!(target: LOG_TARGET,
                            "Send response of process after download file[{file_id}] failed"
                        );
                    });
                }
            }
        }
    }
}

impl Drop for FileMetadataCommand {
    fn drop(&mut self) {
        match self {
            FileMetadataCommand::Read {
                file_id,
                contents_tx,
            } => {
                if let Some(tx) = contents_tx.take() {
                    log::warn!(target: LOG_TARGET,
                        "Send None as response of read file[{file_id}] metadata because of failure"
                    );

                    tx.try_send((FileChunkId::new(*file_id, 0), None))
                        .unwrap_or_else(|e| {
                            log::error!(target: LOG_TARGET,
                                "Send response of read file[{file_id}] metadata failed: {e}"
                            );
                        });
                }
            }
        }
    }
}

impl Drop for FileChunkCommand {
    fn drop(&mut self) {
        match self {
            FileChunkCommand::Read {
                chunk_id,
                contents_tx,
            } => {
                if let Some(tx) = contents_tx.take() {
                    log::warn!(target: LOG_TARGET,
                        "Send None as response of read chunk[{chunk_id}] because of failure"
                    );

                    tx.try_send((*chunk_id, None)).unwrap_or_else(|e| {
                        log::error!(target: LOG_TARGET,
                            "Send response of read chunk[{chunk_id}] failed: {e}"
                        );
                    });
                }
            }
        }
    }
}
