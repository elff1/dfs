use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use super::dfs_grpc::{
    DownloadRequest, DownloadResponse, PublishFileRequest, PublishFileResponse, dfs_server::Dfs,
};
use crate::{
    FileId,
    app::{download::DownloadCommand, fs::FileProcessor, p2p::P2pCommand},
};

const LOG_TARGET: &str = "app::grpc::api";

#[derive(Debug)]
pub(super) struct DfsGrpcService {
    p2p_command_tx: mpsc::Sender<P2pCommand>,
    download_command_tx: mpsc::Sender<DownloadCommand>,
}

impl DfsGrpcService {
    pub(super) fn new(
        p2p_command_tx: mpsc::Sender<P2pCommand>,
        download_command_tx: mpsc::Sender<DownloadCommand>,
    ) -> Self {
        DfsGrpcService {
            p2p_command_tx,
            download_command_tx,
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
                let err_str = format!("Process file [{request_str}] failed: {e}");
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

        self.p2p_command_tx
            .send(P2pCommand::PublishFile(file_result))
            .await
            .map_err(|e| {
                let err_str = format!("Send file process result to P2P service failed: {e}");
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
        log::info!(target: LOG_TARGET, "Got download request: {request:?}");
        let DownloadRequest {
            file_id,
            download_path,
        } = request.into_inner();

        self.download_command_tx
            .send(DownloadCommand::OneFile {
                file_id: FileId::new(file_id),
                download_path: download_path.into(),
            })
            .await
            .map_err(|e| {
                let err_str = format!("Send download file command to download service failed: {e}");
                log::error!(target: LOG_TARGET, "{err_str}");
                Status::internal(err_str)
            })?;

        Ok(Response::new(DownloadResponse {
            success: true,
            error: "".to_string(),
        }))
    }
}
