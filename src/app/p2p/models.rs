use serde::{Deserialize, Serialize};

pub type FilePublishId = FileDownloadId;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileDownloadId {
    pub file_id: u64,
    // 0: metadata, 1..n: chunks
    pub chunk_index: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FileDownloadResponse {
    Success(Vec<u8>),
    Error(String),
}

impl FileDownloadId {
    pub fn new(file_id: u64, chunk_index: usize) -> Self {
        Self {
            file_id,
            chunk_index,
        }
    }
}

impl TryFrom<&FileDownloadId> for Vec<u8> {
    type Error = serde_cbor::Error;

    fn try_from(value: &FileDownloadId) -> Result<Self, Self::Error> {
        serde_cbor::to_vec(value)
    }
}

impl TryFrom<&[u8]> for FileDownloadId {
    type Error = serde_cbor::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(value)
    }
}
