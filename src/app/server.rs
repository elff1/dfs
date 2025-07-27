use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::{
    sync::{Mutex, mpsc},
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::CancellationToken;

use crate::{
    app::grpc::service::{GrpcService, GrpcServiceError},
    app::p2p::{
        config::P2pServiceConfig,
        service::{P2pNetworkError, P2pService},
    },
    file_processor::FileProcessResult,
    file_store::rocksdb::{RocksDb, RocksDbStoreError},
};

const LOG_TARGET: &str = "app::server";
const GRPC_PORT: u16 = 29999;

#[derive(Debug, Error)]
#[allow(dead_code)] // Remove this line if you plan to use all variants
pub enum ServerError {
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

    #[error("P2P network error: {0}")]
    P2pNetwork(#[from] P2pNetworkError),

    #[error("gRPC service error: {0}")]
    GrpcService(#[from] GrpcServiceError),

    #[error("RocksDB store error: {0}")]
    RocksDbStore(#[from] RocksDbStoreError),
}

pub type ServerResult<T> = std::result::Result<T, ServerError>;

type SubtaskHandle = JoinHandle<Result<(), ServerError>>;

pub struct Server {
    cancel_token: CancellationToken,
    subtasks: Arc<Mutex<Vec<SubtaskHandle>>>,
}

#[async_trait]
pub trait Service: Send + Sync + 'static {
    async fn start(self, cancel_token: CancellationToken) -> Result<(), ServerError>;
    //async fn stop(&self) -> Result<(), Error>;
}

impl Server {
    pub fn new() -> Self {
        Self {
            cancel_token: CancellationToken::new(),
            subtasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn start(&self) -> ServerResult<()> {
        let (file_publish_tx, file_publish_rx) = mpsc::channel::<FileProcessResult>(100);

        // P2P service
        let p2p_service = P2pService::new(
            P2pServiceConfig::builder()
                .with_keypair_file("./keys.keypair".into())
                .build(),
            file_publish_rx,
            RocksDb::new("./file_store")?,
        );
        self.spawn_task(p2p_service).await?;

        // gRPC service
        let grpc_service = GrpcService::new(GRPC_PORT, file_publish_tx);
        self.spawn_task(grpc_service).await?;

        Ok(())
    }

    pub async fn spawn_task<S: Service>(&self, service: S) -> ServerResult<()> {
        let mut subtasks = self.subtasks.lock().await;
        let cancel_token = self.cancel_token.clone();
        subtasks.push(tokio::spawn(
            async move { service.start(cancel_token).await },
        ));

        Ok(())
    }

    pub async fn stop(&self) -> ServerResult<()> {
        log::info!(target: LOG_TARGET, "Shutting down server...");

        self.cancel_token.cancel();
        let mut subtasks = self.subtasks.lock().await;
        for subtask in subtasks.iter_mut() {
            subtask.await??;
        }

        Ok(())
    }
}
