use std::{io, sync::Arc};

use async_trait::async_trait;
use thiserror::Error;
use tokio::{
    fs,
    sync::{Mutex, mpsc},
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::CancellationToken;

use super::{
    download::{DownloadCommand, DownloadService, DownloadServiceError},
    fs::{FsChunkCommand, FsCommand, FsService, FsServiceError},
    grpc::{GrpcService, GrpcServiceError},
    p2p::{
        config::P2pServiceConfig,
        {P2pCommand, P2pNetworkError, P2pService},
    },
};
use crate::{
    cli::Cli,
    file_store::rocksdb::{RocksDb, RocksDbStoreError},
};

const LOG_TARGET: &str = "app::server";

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

    #[error("I/O error: {0}")]
    IO(#[from] io::Error),

    #[error("Task join error: {0}")]
    TaskJoin(#[from] JoinError),

    #[error("P2P network error: {0}")]
    P2pNetwork(#[from] P2pNetworkError),

    #[error("gRPC service error: {0}")]
    GrpcService(#[from] GrpcServiceError),

    #[error("Download service error: {0}")]
    DownloadService(#[from] DownloadServiceError),

    #[error("FS service error: {0}")]
    FsService(#[from] FsServiceError),

    #[error("RocksDB store error: {0}")]
    RocksDbStore(#[from] RocksDbStoreError),
}

pub type ServerResult<T> = std::result::Result<T, ServerError>;

type SubtaskHandle = JoinHandle<Result<(), ServerError>>;

pub struct Server {
    cli: Cli,
    cancel_token: CancellationToken,
    subtasks: Arc<Mutex<Vec<SubtaskHandle>>>,
}

#[async_trait]
pub trait Service: Send + Sync + 'static {
    async fn start(self, cancel_token: CancellationToken) -> Result<(), ServerError>;
    //async fn stop(&self) -> Result<(), Error>;
}

impl Server {
    pub fn new(cli: Cli) -> Self {
        Self {
            cli,
            cancel_token: CancellationToken::new(),
            subtasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn init_base_path(&self) -> ServerResult<()> {
        let path = &self.cli.base_path;

        if !fs::try_exists(path).await? {
            fs::create_dir_all(path).await?;
        } else if !fs::metadata(path).await?.is_dir() {
            return Err(ServerError::Start(format!(
                "Path [{}] is not a directory",
                path.display()
            )));
        }

        Ok(())
    }

    pub async fn start(&self) -> ServerResult<()> {
        self.init_base_path().await?;

        let file_store = Arc::new(RocksDb::new(self.cli.base_path.join("file_store"))?);

        // in download service, spawn a download task after a P2pCommand is sent
        let (p2p_command_tx, p2p_command_rx) = mpsc::channel::<P2pCommand>(1);
        let (download_command_tx, download_command_rx) = mpsc::channel::<DownloadCommand>(100);
        let (fs_command_tx, fs_command_rx) = mpsc::channel::<FsCommand>(100);
        let (fs_chunk_command_tx, fs_chunk_command_rx) =
            async_channel::bounded::<FsChunkCommand>(100);

        // FS service
        let fs_service = FsService::new(file_store.clone(), fs_command_rx, fs_chunk_command_rx);
        self.spawn_task(fs_service).await?;

        // Download service
        let download_service = DownloadService::new(file_store.clone(), download_command_rx);
        self.spawn_task(download_service).await?;

        // P2P service
        let p2p_service = P2pService::new(
            P2pServiceConfig::builder()
                .with_keypair_file(self.cli.base_path.join("keys.keypair"))
                .build(),
            file_store,
            fs_command_tx,
            fs_chunk_command_tx,
            p2p_command_rx,
        );
        self.spawn_task(p2p_service).await?;

        // gRPC service
        let grpc_service =
            GrpcService::new(self.cli.grpc_port, p2p_command_tx, download_command_tx);
        self.spawn_task(grpc_service).await?;

        Ok(())
    }

    pub async fn spawn_task<S: Service>(&self, service: S) -> ServerResult<()> {
        let mut subtasks = self.subtasks.lock().await;
        let cancel_token = self.cancel_token.clone();
        subtasks.push(tokio::spawn(service.start(cancel_token)));

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
