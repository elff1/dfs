use std::path::Path;

use rocksdb::{ColumnFamilyDescriptor, Options};
use thiserror::Error;

use crate::file_store::{FileStoreError, PublishedFileRecord, Store};

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
        let key = record.key().to_owned();
        let value: Vec<u8> = record.try_into()?;
        self.db.put_cf(cf, key, value)?;

        Ok(())
    }
}

impl Store for RocksDb {
    fn add_published_file(&self, record: PublishedFileRecord) -> Result<(), FileStoreError> {
        self.add_published_file_inner(record)?;

        Ok(())
    }
}
