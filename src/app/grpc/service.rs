use std::net::AddrParseError;

use async_trait::async_trait;
use publish::{
    PublishFileRequest, PublishFileResponse,
    publish_server::{Publish, PublishServer},
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, transport::Server};

use crate::app::{ServerError, Service};
use crate::file_processor::FileProcessor;

const LOG_TARGET: &str = "app::grpc::service";

#[derive(Debug, Error)]
pub enum GrpcServiceError {
    #[error("SocketAddr parse error: {0}")]
    SocketAddrParse(#[from] AddrParseError),
}

pub mod publish {
    tonic::include_proto!("publish");
}

#[derive(Debug)]
pub struct PublishService {}

impl PublishService {
    pub fn new() -> Self {
        PublishService {}
    }
}

#[tonic::async_trait]
impl Publish for PublishService {
    async fn publish_file(
        &self,
        request: Request<PublishFileRequest>,
    ) -> Result<Response<PublishFileResponse>, Status> {
        log::info!(target: LOG_TARGET, "Got publish request: {request:?}");

        match FileProcessor::publish_file(&request.get_ref().file_path).await {
            Ok(result) => {
                log::info!(target: LOG_TARGET, "File processing result: file[{}], hash[{}]",
                    result.original_file_name, hex::encode(result.merkle_root));

                Ok(Response::new(PublishFileResponse {
                    success: true,
                    error: "".to_string(),
                }))
            }
            Err(e) => {
                let err_msg = e.to_string();

                log::error!(target: LOG_TARGET, "Publish file failed: {err_msg}");

                Ok(Response::new(PublishFileResponse {
                    success: false,
                    error: err_msg,
                }))
            }
        }
    }
}

pub struct GrpcService {
    port: u16,
}

impl GrpcService {
    pub fn new(port: u16) -> Self {
        GrpcService { port }
    }

    async fn start_inner(&self, cancel_token: CancellationToken) -> Result<(), GrpcServiceError> {
        let grpc_address = format!("127.0.0.1:{}", self.port).as_str().parse()?;

        log::info!(target: LOG_TARGET, "gRPC service starting at {grpc_address}");

        Server::builder()
            .add_service(PublishServer::new(PublishService::new()))
            .serve_with_shutdown(grpc_address, cancel_token.cancelled())
            .await
            .unwrap_or_else(|e| log::error!(target: LOG_TARGET, "gRPC run into error {e:?}"));

        log::info!(target: LOG_TARGET, "gRPC service is stutting down...");

        Ok(())
    }
}

#[async_trait]
impl Service for GrpcService {
    async fn start(&self, cancel_token: CancellationToken) -> Result<(), ServerError> {
        self.start_inner(cancel_token).await?;

        Ok(())
    }
}
