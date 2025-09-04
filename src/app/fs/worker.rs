use std::sync::Arc;

use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

use super::{FsChunkCommand, FsHelper, FsServiceError};
use crate::{FileChunkId, file_store};

const LOG_TARGET: &str = "app::fs::worker";

#[derive(Debug)]
pub(super) struct FsServiceWorker<F: file_store::Store + Send + Sync + 'static> {
    pub worker_id: usize,
    pub cancel_token: CancellationToken,
    pub file_store: Arc<F>,
    pub fs_chunk_command_rx: async_channel::Receiver<FsChunkCommand>,
}

impl<F: file_store::Store + Send + Sync + 'static> FsServiceWorker<F> {
    async fn handle_command_read_chunk(
        &self,
        chunk_id: FileChunkId,
        contents_tx: mpsc::Sender<(FileChunkId, Option<Vec<u8>>)>,
    ) -> bool {
        let file_id = chunk_id.file_id;
        let chunk_index = chunk_id.chunk_index;

        let chunks_directory = self
            .file_store
            .get_published_file_chunks_directory(file_id)
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Get file store record of file[{file_id}] failed: {e}");
            });

        let chunk = chunks_directory.and_then(|chunks_directory| {
            FsHelper::read_file_chunk(&chunks_directory, chunk_index).map_err(|e| {
                log::error!(target: LOG_TARGET, "Read chunk[{chunk_id}] at [{}] failed: {e}",
                    chunks_directory.display());
            })
        });

        contents_tx
            .send((FileChunkId::new(file_id, chunk_index), chunk.ok()))
            .await
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Send chunk[{chunk_id}] failed: {e}");
            })
            .is_ok()
    }

    async fn handle_command(&mut self, command: FsChunkCommand) -> Option<()> {
        match command {
            FsChunkCommand::Read {
                chunk_id,
                contents_tx,
            } => {
                self.handle_command_read_chunk(chunk_id, contents_tx).await;
            }
        }

        Some(())
    }

    pub async fn start(mut self) -> Result<(), FsServiceError> {
        log::info!(target: LOG_TARGET, "FS service worker[{}] starting...", self.worker_id);

        loop {
            select! {
                command = self.fs_chunk_command_rx.recv() => {
                    if let Ok(command) = command {
                        self.handle_command(command).await;
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
