use std::path::PathBuf;

use thiserror::Error;

use self::rocksdb::RocksDbStoreError;
use crate::FileId;

mod downloading_file_record;
mod published_file_record;
pub mod rocksdb;
pub use downloading_file_record::*;
pub use published_file_record::*;

#[derive(Debug, Error)]
pub enum FileStoreError {
    #[error("RocksDB store error: {0}")]
    RocksDbStore(#[from] RocksDbStoreError),
    #[error("Published file ID[{0}] not found")]
    PublishedFileNotFound(FileId),
    // #[error("Downloading file ID[{0}] not found")]
    // DownloadingFileNotFound(u64),
}

pub trait Store {
    // published file
    fn add_published_file(&self, record: PublishedFileRecord) -> Result<(), FileStoreError>;
    fn published_file_exists(&self, file_id: FileId) -> Result<bool, FileStoreError>;
    fn get_published_file(
        &self,
        file_id: FileId,
    ) -> Result<Option<PublishedFileRecord>, FileStoreError>;
    fn get_all_published_files(
        &self,
    ) -> Result<impl Iterator<Item = PublishedFileRecord> + Send, FileStoreError>;

    fn get_published_file_chunks_directory(
        &self,
        file_id: FileId,
    ) -> Result<PathBuf, FileStoreError> {
        let record = self
            .get_published_file(file_id)?
            .ok_or(FileStoreError::PublishedFileNotFound(file_id))?;
        Ok(record.chunks_directory)
    }

    // downloading file
    fn add_downloading_file(&self, record: DownloadingFileRecord) -> Result<(), FileStoreError>;
    fn delete_downloading_file(&self, record: FileId) -> Result<(), FileStoreError>;
    fn downloading_file_exists(&self, file_id: FileId) -> Result<bool, FileStoreError>;
    // fn get_downloading_file(
    //     &self,
    //     file_id: FileId,
    // ) -> Result<Option<DownloadingFileRecord>, FileStoreError>;
    fn get_all_downloading_files(
        &self,
    ) -> Result<impl Iterator<Item = DownloadingFileRecord> + Send, FileStoreError>;
}
