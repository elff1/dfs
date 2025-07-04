use std::sync::Arc;

use thiserror::Error;
use tokio::{
    select,
    sync::Mutex,
    task::{JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
#[allow(dead_code)] // Remove this line if you plan to use all variants
pub enum Error {
    #[error("Failed to bind to address: {0}")]
    Bind(String),

    #[error("Failed to start server: {0}")]
    Start(String),

    #[error("Failed to handle request: {0}")]
    Request(String),

    #[error("Failed to send response: {0}")]
    Response(String),

    #[error("Task join error: {0}")]
    TaskJoin(#[from] JoinError),
}

pub type ServerResult<T> = std::result::Result<T, Error>;

pub struct Server {
    cancel_token: CancellationToken,
    subtasks: Arc<Mutex<JoinSet<()>>>,
}

impl Server {
    pub fn new() -> Self {
        Self {
            cancel_token: CancellationToken::new(),
            subtasks: Arc::new(Mutex::new(JoinSet::new())),
        }
    }

    pub async fn start(&self) -> ServerResult<()> {
        let mut join_set = self.subtasks.lock().await;

        let cancel_token = self.cancel_token.clone();
        join_set.spawn(async move {
            loop {
                select! {
                    _ = cancel_token.cancelled() => {
                        println!("Stopping tasks...");
                        break;
                    }
                    // Placeholder for handling incoming requests
                    // This would typically involve listening on a socket and processing requests
                }
            }
        });

        Ok(())
    }

    pub async fn stop(&self) -> ServerResult<()> {
        println!("Shutting down server...");
        self.cancel_token.cancel();
        let mut subtasks = self.subtasks.lock().await;
        while let Some(res) = subtasks.join_next().await {
            res?;
        }
        Ok(())
    }
}
