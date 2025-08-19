use std::borrow::Cow;

use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use super::dfs_grpc::{
    DownloadRequest, DownloadResponse, PublishFileRequest, PublishFileResponse, dfs_server::Dfs,
};
use crate::app::{
    download::DownloadCommand,
    fs::{FileProcessResult, FileProcessResultHash, FileProcessor},
    p2p::{FileChunkId, P2pCommand},
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

impl DfsGrpcService {
    async fn download_inner(
        &self,
        DownloadRequest {
            file_id,
            download_path,
        }: DownloadRequest,
    ) -> Result<(), Cow<'_, str>> {
        let (tx, rx) = oneshot::channel();
        self.p2p_command_tx
            .send(P2pCommand::DownloadFile {
                id: FileChunkId::new(file_id, 0),
                tx,
            })
            .await
            .map_err(|e| format!("Send metadata request to P2P service failed: {e}"))?;

        let metadata: Box<FileProcessResult> = rx
            .await
            .map_err(|e| format!("Receive metadata from P2P service failed: {e}"))?
            .ok_or("Download [None] metadata")?
            .as_slice()
            .try_into()
            .map_err(|e| format!("Parse metadata failed: {e}"))?;

        log::info!(target: LOG_TARGET, "Downloaded metadata of file[{file_id} | {}] with {} chunks",
            metadata.original_file_name, metadata.number_of_chunks);

        self.download_command_tx
            .send(DownloadCommand::OneFile {
                id: FileProcessResultHash::new(file_id),
                metadata,
                download_path: download_path.into(),
            })
            .await
            .map_err(|e| format!("Send download file command to download service failed: {e}"))?;

        Ok(())
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

        if let Err(err_str) = self.download_inner(request.into_inner()).await {
            log::error!(target: LOG_TARGET, "{err_str}");
            return Err(Status::internal(err_str));
        }

        Ok(Response::new(DownloadResponse {
            success: true,
            error: "".to_string(),
        }))
    }
}
