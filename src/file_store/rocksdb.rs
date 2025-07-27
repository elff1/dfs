use std::path::Path;

use rocksdb::{ColumnFamilyDescriptor, Options};
use thiserror::Error;

use crate::file_store::{FileProcessResultHash, FileStoreError, PublishedFileRecord, Store};

const LOG_TARGET: &str = "file_store::rocksdb";
const PUBLISHED_FILES_COLUMN_FAMILY_NAME: &str = "published_files";

pub struct RocksDb {
    db: rocksdb::DB,
}

#[derive(Debug, Error)]
pub enum RocksDbStoreError {
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),
    #[error("Column family does not exist: {0}")]
    ColumnFamilyMissing(String),
    #[error("Cbor error: {0}")]
    Cbor(#[from] serde_cbor::Error),
    // #[error("Invalid file ID: {}", hex::encode(.0))]
    // InvalidFileId(Vec<u8>),
}

impl RocksDb {
    pub fn new<P: AsRef<Path>>(folder: P) -> Result<Self, RocksDbStoreError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs = ColumnFamilyDescriptor::new(PUBLISHED_FILES_COLUMN_FAMILY_NAME, opts.clone());
        Ok(Self {
            db: rocksdb::DB::open_cf_descriptors(&opts, folder, vec![cfs])?,
        })
    }

    fn add_published_file_inner(
        &self,
        record: PublishedFileRecord,
    ) -> Result<(), RocksDbStoreError> {
        let cf = self
            .db
            .cf_handle(PUBLISHED_FILES_COLUMN_FAMILY_NAME)
            .ok_or(RocksDbStoreError::ColumnFamilyMissing(
                PUBLISHED_FILES_COLUMN_FAMILY_NAME.to_string(),
            ))?;
        let key = record.key();
        let value: Vec<u8> = record.try_into()?;
        self.db.put_cf(cf, key, value)?;

        Ok(())
    }

    fn published_file_exists_inner(&self, file_id: u64) -> Result<bool, RocksDbStoreError> {
        let cf = self
            .db
            .cf_handle(PUBLISHED_FILES_COLUMN_FAMILY_NAME)
            .ok_or(RocksDbStoreError::ColumnFamilyMissing(
                PUBLISHED_FILES_COLUMN_FAMILY_NAME.to_string(),
            ))?;
        Ok(self
            .db
            .full_iterator_cf(cf, rocksdb::IteratorMode::Start)
            .filter_map(|res| res.ok())
            .filter_map(|(key, _)| {
                FileProcessResultHash::try_from(key.as_ref())
                    .inspect_err(|_| log::debug!(target: LOG_TARGET, "Invalid file ID in RocksDB: {}", hex::encode(key)))
                    .ok()
            })
            .any(|key| key.inner() == file_id)
        )
    }

    fn get_published_file_inner(
        &self,
        file_id: u64,
    ) -> Result<Option<PublishedFileRecord>, RocksDbStoreError> {
        let cf = self
            .db
            .cf_handle(PUBLISHED_FILES_COLUMN_FAMILY_NAME)
            .ok_or(RocksDbStoreError::ColumnFamilyMissing(
                PUBLISHED_FILES_COLUMN_FAMILY_NAME.to_string(),
            ))?;
        Ok(self
            .db
            .get_cf(cf, FileProcessResultHash::new(file_id).to_vec())?
            .map(PublishedFileRecord::try_from)
            .transpose()?)
    }
}

impl Store for RocksDb {
    fn add_published_file(&self, record: PublishedFileRecord) -> Result<(), FileStoreError> {
        Ok(self.add_published_file_inner(record)?)
    }

    fn published_file_exists(&self, file_id: u64) -> Result<bool, FileStoreError> {
        Ok(self.published_file_exists_inner(file_id)?)
    }

    fn get_published_file(
        &self,
        file_id: u64,
    ) -> Result<Option<PublishedFileRecord>, FileStoreError> {
        Ok(self.get_published_file_inner(file_id)?)
    }
}
