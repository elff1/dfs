use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    file_processor::{FileProcessResult, FileProcessResultHash},
    file_store::rocksdb::RocksDbStoreError,
};

pub mod rocksdb;

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishedFileRecord {
    pub id: FileProcessResultHash,
    pub original_file_name: String,
    pub chunks_directory: PathBuf,
    pub public: bool,
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

impl TryInto<Vec<u8>> for PublishedFileRecord {
    type Error = serde_cbor::Error;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        serde_cbor::to_vec(&self)
    }
}

impl TryFrom<Vec<u8>> for PublishedFileRecord {
    type Error = serde_cbor::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(&value)
    }
}

#[derive(Debug, Error)]
pub enum FileStoreError {
    #[error("RocksDB store error: {0}")]
    RocksDbStore(#[from] RocksDbStoreError),
    #[error("Published file ID[{0}] not found")]
    PublishedFileNotFound(u64),
}

pub trait Store {
    fn add_published_file(&self, record: PublishedFileRecord) -> Result<(), FileStoreError>;
    fn published_file_exists(&self, file_id: u64) -> Result<bool, FileStoreError>;
    fn get_published_file(
        &self,
        file_id: u64,
    ) -> Result<Option<PublishedFileRecord>, FileStoreError>;

    fn get_published_file_chunks_directory(&self, file_id: u64) -> Result<PathBuf, FileStoreError> {
        let record = self
            .get_published_file(file_id)?
            .ok_or(FileStoreError::PublishedFileNotFound(file_id))?;
        Ok(record.chunks_directory)
    }
}
