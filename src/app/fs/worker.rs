use std::sync::Arc;

use tokio::select;
use tokio_util::sync::CancellationToken;

use super::{
    FileChunkCommand, FileMetadataCommand, FileProcessCommand, FileProcessor, FsHelper,
    FsServiceError,
};
use crate::{
    FileChunkId,
    file_store::{self, PublishedFileRecord},
};

const LOG_TARGET: &str = "app::fs::worker";

#[derive(Debug)]
pub(super) struct FsServiceWorker<F: file_store::Store + Send + Sync + 'static> {
    pub worker_id: usize,
    pub cancel_token: CancellationToken,
    pub file_store: Arc<F>,
    pub file_process_command_rx: async_channel::Receiver<FileProcessCommand>,
    pub file_metadata_command_rx: async_channel::Receiver<FileMetadataCommand>,
    pub file_chunk_command_rx: async_channel::Receiver<FileChunkCommand>,
}

impl<F: file_store::Store + Send + Sync + 'static> FsServiceWorker<F> {
    async fn handle_file_process_command(&mut self, mut command: FileProcessCommand) -> Option<()> {
        match command {
            FileProcessCommand::BeforePublish {
                ref file_path,
                public,
                ref mut metadata_tx,
            } => {
                if metadata_tx.is_none() {
                    log::error!(target: LOG_TARGET, "Process file[{file_path}] before publish failed: no metadata_tx in command");
                    return None;
                }

                let process_result = FileProcessor::process_file(file_path, public)
                    .map_err(|e| {
                        log::error!(target: LOG_TARGET, "Process file[{file_path}] before publish failed: {e}");
                    })
                    .ok()?;

                self.file_store
                    .add_published_file(PublishedFileRecord {
                        file_id: process_result.metadata.file_id,
                        original_file_name: process_result.metadata.original_file_name.clone(),
                        chunks_directory: process_result.chunks_directory,
                        public: process_result.metadata.public,
                    })
                    .map_err(|e| {
                        log::error!(target: LOG_TARGET, "Add new published file[{file_path}] to file store failed: {e}")
                    })
                    .ok()?;

                metadata_tx.take()?
                    .send(Some(process_result.metadata))
                    .map_err(|e| {
                        log::error!(target: LOG_TARGET, "Send metadata of file[{file_path}] failed: {e:?}");
                    })
                    .ok()?;
            }
            FileProcessCommand::AfterDownload {
                ref mut chunks_directory,
                file_id,
                ref mut original_file_name,
                number_of_chunks: _,
                public,
                ref mut process_result_tx,
            } => {
                if process_result_tx.is_none() {
                    return None;
                }
                let chunks_directory = chunks_directory.take()?;
                let original_file_name = original_file_name.take()?;

                self.file_store
                    .add_published_file(PublishedFileRecord {
                        file_id,
                        original_file_name,
                        chunks_directory,
                        public,
                    })
                    .map_err(
                        |e| log::error!(target: LOG_TARGET, "Add publish file to file store failed: {e}"),
                    )
                    .ok()?;
            }
        }

        Some(())
    }

    async fn read_metadata_or_chunk(&self, chunk_id: &FileChunkId) -> Option<Vec<u8>> {
        let file_id = chunk_id.file_id;
        let chunk_index = chunk_id.chunk_index;

        let chunks_directory = self
            .file_store
            .get_published_file_chunks_directory(file_id)
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Get file store record of file[{file_id}] failed: {e}");
            })
            .ok()?;

        FsHelper::read_file_chunk(&chunks_directory, chunk_index)
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Read chunk[{chunk_id}] at [{}] failed: {e}",
                    chunks_directory.display());
            })
            .ok()
    }

    async fn handle_file_metadata_command(
        &mut self,
        mut command: FileMetadataCommand,
    ) -> Option<()> {
        match command {
            FileMetadataCommand::Read {
                file_id,
                ref mut contents_tx,
            } => {
                let chunk_id = FileChunkId::new(file_id, 0);
                let metadata = self.read_metadata_or_chunk(&chunk_id).await?;

                contents_tx
                    .take()?
                    .send((chunk_id, Some(metadata)))
                    .await
                    .map_err(|e| {
                        log::error!(target: LOG_TARGET, "Send metadata of file[{file_id}] failed: {e}");
                    })
                    .ok()?;
            }
        }

        Some(())
    }

    async fn handle_file_chunk_command(&mut self, mut command: FileChunkCommand) -> Option<()> {
        match command {
            FileChunkCommand::Read {
                chunk_id,
                ref mut contents_tx,
            } => {
                let chunk = self.read_metadata_or_chunk(&chunk_id).await?;

                contents_tx
                    .take()?
                    .send((chunk_id, Some(chunk)))
                    .await
                    .map_err(|e| {
                        log::error!(target: LOG_TARGET, "Send chunk[{chunk_id}] failed: {e}");
                    })
                    .ok()?;
            }
        }

        Some(())
    }

    pub async fn start(mut self) -> Result<(), FsServiceError> {
        log::info!(target: LOG_TARGET, "FS service worker[{}] starting...", self.worker_id);

        loop {
            select! {
                command = self.file_process_command_rx.recv() => {
                    if let Ok(command) = command {
                        self.handle_file_process_command(command).await;
                    }
                }
                command = self.file_metadata_command_rx.recv() => {
                    if let Ok(command) = command {
                        self.handle_file_metadata_command(command).await;
                    }
                }
                command = self.file_chunk_command_rx.recv() => {
                    if let Ok(command) = command {
                        self.handle_file_chunk_command(command).await;
                    }
                }
                _ = self.cancel_token.cancelled() => {
                    log::info!(target: LOG_TARGET, "FS service worker[{}] is shutting down...", self.worker_id);
                    break;
                }
            }
        }

        Ok(())
    }
}
