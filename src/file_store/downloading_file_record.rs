use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::file_processor::FileProcessResultHash;

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadingFileRecord {
    pub id: FileProcessResultHash,
    pub original_file_name: String,
    pub download_directory: PathBuf,
}

impl DownloadingFileRecord {
    pub fn key(&self) -> Vec<u8> {
        self.id.into()
    }
}

impl TryFrom<DownloadingFileRecord> for Vec<u8> {
    type Error = serde_cbor::Error;

    fn try_from(value: DownloadingFileRecord) -> Result<Self, Self::Error> {
        serde_cbor::to_vec(&value)
    }
}

impl TryFrom<&[u8]> for DownloadingFileRecord {
    type Error = serde_cbor::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(value)
    }
}
