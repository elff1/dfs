use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::{
    sync::Mutex,
    task::{JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;

use super::P2pService;

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
    subtasks: Arc<Mutex<JoinSet<Result<(), Error>>>>,
}

#[async_trait]
pub trait Service: Send + Sync + 'static {
    async fn start(&self, cancel_token: CancellationToken) -> Result<(), Error>;
    //async fn stop(&self) -> Result<(), Error>;
}

impl Server {
    pub fn new() -> Self {
        Self {
            cancel_token: CancellationToken::new(),
            subtasks: Arc::new(Mutex::new(JoinSet::new())),
        }
    }

    pub async fn start(&self) -> ServerResult<()> {
        let p2p_service = P2pService::new();
        self.spawn_task(p2p_service).await?;

        // let mut subtasks = self.subtasks.lock().await;
        // let cancel_token = self.cancel_token.clone();
        // subtasks.spawn(async move {
        //     loop {
        //         select! {
        //             _ = cancel_token.cancelled() => {
        //                 println!("Stopping tasks...");
        //                 break;
        //             }
        //             // Placeholder for handling incoming requests
        //             // This would typically involve listening on a socket and processing requests
        //         }
        //     }
        //     Ok(())
        // });

        Ok(())
    }

    pub async fn spawn_task<S: Service>(&self, service: S) -> ServerResult<()> {
        let mut subtasks = self.subtasks.lock().await;
        let cancel_token = self.cancel_token.clone();
        subtasks.spawn(async move { service.start(cancel_token).await });

        Ok(())
    }

    pub async fn stop(&self) -> ServerResult<()> {
        println!("Shutting down server...");
        self.cancel_token.cancel();
        let mut subtasks = self.subtasks.lock().await;
        while let Some(res) = subtasks.join_next().await {
            res??;
        }
        Ok(())
    }
}
