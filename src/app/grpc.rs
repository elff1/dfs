use std::net::AddrParseError;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use self::{api::DfsGrpcService, dfs_grpc::dfs_server::DfsServer};
use super::{ServerError, Service, p2p::P2pCommand};
use crate::file_processor::FileProcessResult;

mod dfs_grpc {
    tonic::include_proto!("dfs_grpc");
}
mod api;

const LOG_TARGET: &str = "app::grpc";

#[derive(Debug, Error)]
pub enum GrpcServiceError {
    #[error("SocketAddr parse error: {0}")]
    SocketAddrParse(#[from] AddrParseError),
}

pub struct GrpcService {
    port: u16,
    file_publish_tx: mpsc::Sender<FileProcessResult>,
    p2p_command_tx: mpsc::Sender<P2pCommand>,
}

impl GrpcService {
    pub fn new(
        port: u16,
        file_publish_tx: mpsc::Sender<FileProcessResult>,
        p2p_command_tx: mpsc::Sender<P2pCommand>,
    ) -> Self {
        GrpcService {
            port,
            file_publish_tx,
            p2p_command_tx,
        }
    }

    async fn start_inner(self, cancel_token: CancellationToken) -> Result<(), GrpcServiceError> {
        let grpc_address = format!("127.0.0.1:{}", self.port).as_str().parse()?;

        log::info!(target: LOG_TARGET, "gRPC service starting at {grpc_address}");

        Server::builder()
            .add_service(DfsServer::new(DfsGrpcService::new(
                self.file_publish_tx,
                self.p2p_command_tx,
            )))
            .serve_with_shutdown(grpc_address, cancel_token.cancelled())
            .await
            .unwrap_or_else(|e| log::error!(target: LOG_TARGET, "gRPC run into error: {e:?}"));

        log::info!(target: LOG_TARGET, "gRPC service is stutting down...");

        Ok(())
    }
}

#[async_trait]
impl Service for GrpcService {
    async fn start(self, cancel_token: CancellationToken) -> Result<(), ServerError> {
        self.start_inner(cancel_token).await?;

        Ok(())
    }
}
