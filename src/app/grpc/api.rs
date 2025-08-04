use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use super::dfs_grpc::{
    DownloadRequest, DownloadResponse, PublishFileRequest, PublishFileResponse, dfs_server::Dfs,
};
use crate::{
    app::p2p::{MetadataDownloadRequest, P2pCommand},
    file_processor::{FileProcessResult, FileProcessor},
};

const LOG_TARGET: &str = "app::grpc::api";

#[derive(Debug)]
pub(super) struct DfsGrpcService {
    file_publish_tx: mpsc::Sender<FileProcessResult>,
    p2p_command_sender: mpsc::Sender<P2pCommand>,
}

impl DfsGrpcService {
    pub(super) fn new(
        file_publish_tx: mpsc::Sender<FileProcessResult>,
        p2p_command_sender: mpsc::Sender<P2pCommand>,
    ) -> Self {
        DfsGrpcService {
            file_publish_tx,
            p2p_command_sender,
        }
    }
}

#[tonic::async_trait]
impl Dfs for DfsGrpcService {
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

        log::info!(target: LOG_TARGET, "File process result: file[{}], root hash[{}]",
            file_result.original_file_name, hex::encode(file_result.merkle_root.as_ref()));

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

    async fn download(
        &self,
        request: Request<DownloadRequest>,
    ) -> Result<Response<DownloadResponse>, Status> {
        let request = request.into_inner();
        let (tx, rx) = oneshot::channel();
        self.p2p_command_sender
            .send(P2pCommand::RequestMetadata {
                request: MetadataDownloadRequest {
                    file_id: request.file_id,
                },
                tx,
            })
            .await
            .map_err(|e| {
                let err_str = format!("Send metadata request to P2P service failed: {e:?}");
                log::error!(target: LOG_TARGET, "{err_str}");
                Status::internal(err_str)
            })?;

        let metadata = rx.await.map_err(|e| {
            let err_str = format!("Receive metadata from P2P service failed: {e:?}");
            log::error!(target: LOG_TARGET, "{err_str}");
            Status::internal(err_str)
        })?;

        log::info!(target: LOG_TARGET, "Download metadata of [{}]", metadata.map_or("".to_string(), |m| m.original_file_name));

        Ok(Response::new(DownloadResponse {
            success: true,
            error: "".to_string(),
        }))
    }
}
