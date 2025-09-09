use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use super::dfs_grpc::{
    DownloadRequest, DownloadResponse, PublishFileRequest, PublishFileResponse, dfs_server::Dfs,
};
use crate::{
    FileId,
    app::{download::DownloadCommand, fs::FsCommand, p2p::P2pCommand},
};

const LOG_TARGET: &str = "app::grpc::api";

#[derive(Debug)]
pub(super) struct DfsGrpcService {
    fs_command_tx: mpsc::Sender<FsCommand>,
    p2p_command_tx: mpsc::Sender<P2pCommand>,
    download_command_tx: mpsc::Sender<DownloadCommand>,
}

impl DfsGrpcService {
    pub(super) fn new(
        fs_command_tx: mpsc::Sender<FsCommand>,
        p2p_command_tx: mpsc::Sender<P2pCommand>,
        download_command_tx: mpsc::Sender<DownloadCommand>,
    ) -> Self {
        DfsGrpcService {
            fs_command_tx,
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
        let PublishFileRequest { file_path, public } = request.into_inner();
        log::info!(target: LOG_TARGET, "Get publish file request: {request_str}");

        let (tx, rx) = oneshot::channel();
        self.fs_command_tx
            .send(FsCommand::ProcessFileBeforePublish {
                file_path: file_path.clone(),
                public,
                metadata_tx: tx,
            })
            .await
            .map_err(|e| {
                let err_str = format!("Send command to FS service failed: {e}");
                log::error!(target: LOG_TARGET, "{err_str}");
                Status::internal(err_str)
            })?;

        let metadata = rx
            .await
            .map_err(|e| {
                let err_str =
                    format!("Receive metadata of file[{file_path}] from FS service failed: {e}");
                log::error!(target: LOG_TARGET, "{err_str}");
                Status::internal(err_str)
            })?
            .ok_or_else(|| {
                let err_str = format!("Process file[{file_path}] before publish failed");
                log::error!(target: LOG_TARGET, "{err_str}");
                Status::internal(err_str)
            })?;

        self.p2p_command_tx
            .send(P2pCommand::PublishFile(metadata))
            .await
            .map_err(|e| {
                let err_str = format!("Send publish file command to P2P service failed: {e}");
                log::error!(target: LOG_TARGET, "{err_str}");
                Status::internal(err_str)
            })?;

        Ok(Response::new(PublishFileResponse {
            success: true,
            error: String::default(),
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
            error: String::default(),
        }))
    }
}
