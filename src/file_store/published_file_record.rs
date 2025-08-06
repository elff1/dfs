use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::file_processor::{FileProcessResult, FileProcessResultHash};

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishedFileRecord {
    pub id: FileProcessResultHash,
    pub original_file_name: String,
    pub chunks_directory: PathBuf,
    pub public: bool,
}

impl PublishedFileRecord {
    // pub fn new(original_file_name: String, chunks_dirctory: PathBuf, public: bool) -> Self {
    //     Self {
    //         original_file_name,
    //         chunks_dirctory,
    //         public,
    //     }
    // }

    pub fn key(&self) -> Vec<u8> {
        self.id.into()
    }
}

impl From<FileProcessResult> for PublishedFileRecord {
    fn from(result: FileProcessResult) -> Self {
        Self {
            id: result.hash_sha256(),
            original_file_name: result.original_file_name,
            chunks_directory: result.chunks_directory,
            public: result.public,
        }
    }
}

impl From<&FileProcessResult> for PublishedFileRecord {
    fn from(result: &FileProcessResult) -> Self {
        Self {
            id: result.hash_sha256(),
            original_file_name: result.original_file_name.clone(),
            chunks_directory: result.chunks_directory.clone(),
            public: result.public,
        }
    }
}

impl TryFrom<PublishedFileRecord> for Vec<u8> {
    type Error = serde_cbor::Error;

    fn try_from(value: PublishedFileRecord) -> Result<Self, Self::Error> {
        serde_cbor::to_vec(&value)
    }
}

impl TryFrom<&[u8]> for PublishedFileRecord {
    type Error = serde_cbor::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(value)
    }
}
