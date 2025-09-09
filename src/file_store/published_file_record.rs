use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::FileId;

const LOG_TARGET: &str = "file_store::published_file_record";

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishedFileRecord {
    pub file_id: FileId,
    pub original_file_name: String,
    pub chunks_directory: PathBuf,
    pub public: bool,
}

impl PublishedFileRecord {
    pub fn key(&self) -> Vec<u8> {
        self.file_id.into()
    }
}

impl From<&PublishedFileRecord> for Vec<u8> {
    fn from(value: &PublishedFileRecord) -> Self {
        serde_cbor::to_vec(value)
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "serde_cbor::to_vec(PublishedFileRecord[{value:?}]) failed: {e}");
            })
            .unwrap()
    }
}

impl TryFrom<&[u8]> for PublishedFileRecord {
    type Error = serde_cbor::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(value)
    }
}
