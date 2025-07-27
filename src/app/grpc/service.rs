use std::net::AddrParseError;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, transport::Server};

use crate::{
    app::{
        ServerError, Service,
        grpc::publish::{
            PublishFileRequest, PublishFileResponse,
            publish_server::{Publish, PublishServer},
        },
    },
    file_processor::{FileProcessResult, FileProcessor},
};

const LOG_TARGET: &str = "app::grpc::service";

#[derive(Debug, Error)]
pub enum GrpcServiceError {
    #[error("SocketAddr parse error: {0}")]
    SocketAddrParse(#[from] AddrParseError),
}

#[derive(Debug)]
struct PublishService {
    file_publish_tx: mpsc::Sender<FileProcessResult>,
}

impl PublishService {
    pub fn new(file_publish_tx: mpsc::Sender<FileProcessResult>) -> Self {
        PublishService { file_publish_tx }
    }
}

#[tonic::async_trait]
impl Publish for PublishService {
    async fn publish_file(
        &self,
        request: Request<PublishFileRequest>,
    ) -> Result<Response<PublishFileResponse>, Status> {
        let request_str = format!("{request:?}");
        log::info!(target: LOG_TARGET, "Got publish request: {request_str}");

        let PublishFileRequest { file_path, public } = request.into_inner();
        let file_result = FileProcessor::process_file(file_path, public)
            .await
            .map_err(|e| {
                let err_str = format!("Process file [{request_str}] failed: {e:?}");
                log::error!(target: LOG_TARGET, "{err_str}");

                use std::io::ErrorKind as K;
                match e.kind() {
                    K::NotFound => Status::not_found(request_str),
                    K::PermissionDenied => Status::permission_denied(request_str),
                    K::InvalidFilename => Status::invalid_argument(request_str),
                    _ => Status::internal(err_str),
                }
            })?;

        log::info!(target: LOG_TARGET, "File process result: file[{}], hash[{}]",
            file_result.original_file_name, hex::encode(file_result.merkle_root));

        self.file_publish_tx.send(file_result).await.map_err(|e| {
            let err_str = format!("Send file process result to P2P service failed: {e:?}");
            log::error!(target: LOG_TARGET, "{err_str}");
            Status::internal(err_str)
        })?;

        Ok(Response::new(PublishFileResponse {
            success: true,
            error: "".to_string(),
        }))
    }
}

pub struct GrpcService {
    port: u16,
    file_publish_tx: mpsc::Sender<FileProcessResult>,
}

impl GrpcService {
    pub fn new(port: u16, file_publish_tx: mpsc::Sender<FileProcessResult>) -> Self {
        GrpcService {
            port,
            file_publish_tx,
        }
    }

    async fn start_inner(self, cancel_token: CancellationToken) -> Result<(), GrpcServiceError> {
        let grpc_address = format!("127.0.0.1:{}", self.port).as_str().parse()?;

        log::info!(target: LOG_TARGET, "gRPC service starting at {grpc_address}");

        Server::builder()
            .add_service(PublishServer::new(PublishService::new(
                self.file_publish_tx,
            )))
            .serve_with_shutdown(grpc_address, cancel_token.cancelled())
            .await
            .unwrap_or_else(|e| log::error!(target: LOG_TARGET, "gRPC run into error {e:?}"));

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
