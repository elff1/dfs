use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct FileChunkId {
    pub file_id: u64,
    /// 0: metadata, 1..n: chunks
    pub chunk_index: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FileDownloadResponse {
    Success(Vec<u8>),
    Error(String),
}

impl FileChunkId {
    pub fn new(file_id: u64, chunk_index: usize) -> Self {
        Self {
            file_id,
            chunk_index,
        }
    }
}

impl From<&FileChunkId> for Vec<u8> {
    fn from(value: &FileChunkId) -> Self {
        serde_cbor::to_vec(value)
            .map_err(|e| {
                log::error!("serde_cbor::to_vec(FileChunkId[{value}]) failed: {e}");
            })
            .unwrap()
    }
}

impl TryFrom<&[u8]> for FileChunkId {
    type Error = serde_cbor::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(value)
    }
}

impl std::fmt::Display for FileChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}/{}", self.file_id, self.chunk_index))
    }
}
