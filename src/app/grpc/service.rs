use std::net::AddrParseError;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, transport::Server};

use super::publish::{
    PublishFileRequest, PublishFileResponse,
    publish_server::{Publish, PublishServer},
};
use crate::{
    app::{ServerError, Service},
    file_processor::{FileProcessor, FileProcessorResult},
};

const LOG_TARGET: &str = "app::grpc::service";

#[derive(Debug, Error)]
pub enum GrpcServiceError {
    #[error("SocketAddr parse error: {0}")]
    SocketAddrParse(#[from] AddrParseError),
}

#[derive(Debug)]
struct PublishService {
    file_publish_tx: mpsc::Sender<FileProcessorResult>,
}

impl PublishService {
    pub fn new(file_publish_tx: mpsc::Sender<FileProcessorResult>) -> Self {
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

        let err_msg = match FileProcessor::publish_file(request.into_inner()).await {
            Ok(result) => {
                log::info!(target: LOG_TARGET, "File process result: file[{}], hash[{}]",
                    result.original_file_name, hex::encode(result.merkle_root));

                if let Err(e) = self.file_publish_tx.send(result).await {
                    format!("Send file process result to P2P service failed [{e:?}]")
                } else {
                    return Ok(Response::new(PublishFileResponse {
                        success: true,
                        error: "".to_string(),
                    }));
                }
            }
            Err(e) => {
                format!("Process file [{request_str}] failed [{e:?}]")
            }
        };

        log::error!(target: LOG_TARGET, "{err_msg}");
        Err(Status::internal(err_msg))
    }
}

pub struct GrpcService {
    port: u16,
    file_publish_tx: mpsc::Sender<FileProcessorResult>,
}

impl GrpcService {
    pub fn new(port: u16, file_publish_tx: mpsc::Sender<FileProcessorResult>) -> Self {
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
