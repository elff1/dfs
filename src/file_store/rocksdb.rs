use std::path::Path;

use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options};
use thiserror::Error;

use crate::file_store::{
    DownloadingFileRecord, FileProcessResultHash, FileStoreError, PublishedFileRecord, Store,
};

const LOG_TARGET: &str = "file_store::rocksdb";
const PUBLISHED_FILES_COLUMN_FAMILY_NAME: &str = "published_files";
const DOWNLOADING_FILES_COLUMN_FAMILY_NAME: &str = "downloading_files";

#[derive(Debug, Error)]
pub enum RocksDbStoreError {
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),
    #[error("Column family does not exist: {0}")]
    ColumnFamilyMissing(String),
    #[error("Cbor error: {0}")]
    Cbor(#[from] serde_cbor::Error),
    // #[error("Invalid file ID: {}", hex::encode(.0.as_ref()))]
    // InvalidFileId(Vec<u8>),
}

pub struct RocksDb {
    db: rocksdb::DB,
}

impl RocksDb {
    pub fn new<P: AsRef<Path>>(folder: P) -> Result<Self, RocksDbStoreError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let published_files_cfs =
            ColumnFamilyDescriptor::new(PUBLISHED_FILES_COLUMN_FAMILY_NAME, opts.clone());
        let downloading_files_cfs =
            ColumnFamilyDescriptor::new(DOWNLOADING_FILES_COLUMN_FAMILY_NAME, opts.clone());
        Ok(Self {
            db: rocksdb::DB::open_cf_descriptors(
                &opts,
                folder,
                vec![published_files_cfs, downloading_files_cfs],
            )?,
        })
    }

    fn column_family(&self, cf_name: &str) -> Result<&ColumnFamily, RocksDbStoreError> {
        self.db
            .cf_handle(cf_name)
            .ok_or(RocksDbStoreError::ColumnFamilyMissing(cf_name.to_string()))
    }

    fn add_published_file_inner(
        &self,
        record: PublishedFileRecord,
    ) -> Result<(), RocksDbStoreError> {
        let cf = self.column_family(PUBLISHED_FILES_COLUMN_FAMILY_NAME)?;
        let key = record.key();
        let value: Vec<u8> = (&record).into();
        self.db.put_cf(cf, key, value)?;

        Ok(())
    }

    fn published_file_exists_inner(&self, file_id: u64) -> Result<bool, RocksDbStoreError> {
        let cf = self.column_family(PUBLISHED_FILES_COLUMN_FAMILY_NAME)?;
        Ok(self
            .db
            .key_may_exist_cf(cf, FileProcessResultHash::new(file_id).to_array()))
    }

    fn get_published_file_inner(
        &self,
        file_id: u64,
    ) -> Result<Option<PublishedFileRecord>, RocksDbStoreError> {
        let cf = self.column_family(PUBLISHED_FILES_COLUMN_FAMILY_NAME)?;
        Ok(self
            .db
            .get_cf(cf, FileProcessResultHash::new(file_id).to_array())?
            .map(|value| value.as_slice().try_into())
            .transpose()?)
    }

    fn get_all_published_file_inner(
        &self,
    ) -> Result<impl Iterator<Item = PublishedFileRecord> + Send, RocksDbStoreError> {
        let cf = self.column_family(PUBLISHED_FILES_COLUMN_FAMILY_NAME)?;
        Ok(self
            .db
            .iterator_cf(cf, rocksdb::IteratorMode::Start)
            .filter_map(|record| {
                record
                    .map_err(|e| {
                        log::warn!(target: LOG_TARGET, "Get all published files, find DB error: {e}");
                    })
                    .ok()
            })
            .filter_map(|(_, value)| {
                value
                    .as_ref()
                    .try_into()
                    .map_err(|e| {
                        log::warn!(target: LOG_TARGET, "Get all published files, find cbor error: {e}");
                    })
                    .ok()
            })
        )
    }

    fn add_downloading_file_inner(
        &self,
        record: DownloadingFileRecord,
    ) -> Result<(), RocksDbStoreError> {
        let cf = self.column_family(DOWNLOADING_FILES_COLUMN_FAMILY_NAME)?;
        let key = record.key();
        let value: Vec<u8> = (&record).into();
        self.db.put_cf(cf, key, value)?;

        Ok(())
    }

    fn downloading_file_exists_inner(&self, file_id: u64) -> Result<bool, RocksDbStoreError> {
        let cf = self.column_family(DOWNLOADING_FILES_COLUMN_FAMILY_NAME)?;
        Ok(self
            .db
            .key_may_exist_cf(cf, FileProcessResultHash::new(file_id).to_array()))
    }

    fn get_downloading_file_inner(
        &self,
        file_id: u64,
    ) -> Result<Option<DownloadingFileRecord>, RocksDbStoreError> {
        let cf = self.column_family(DOWNLOADING_FILES_COLUMN_FAMILY_NAME)?;
        Ok(self
            .db
            .get_cf(cf, FileProcessResultHash::new(file_id).to_array())?
            .map(|value| value.as_slice().try_into())
            .transpose()?)
    }

    fn get_all_downloading_file_inner(
        &self,
    ) -> Result<impl Iterator<Item = DownloadingFileRecord> + Send, RocksDbStoreError> {
        let cf = self.column_family(DOWNLOADING_FILES_COLUMN_FAMILY_NAME)?;
        Ok(self
            .db
            .iterator_cf(cf, rocksdb::IteratorMode::Start)
            .filter_map(|record| {
                record
                    .map_err(|e| {
                        log::warn!(target: LOG_TARGET, "Get all published files, find DB error: {e}");
                    })
                    .ok()
            })
            .filter_map(|(_, value)| {
                value
                    .as_ref()
                    .try_into()
                    .map_err(|e| {
                        log::warn!(target: LOG_TARGET, "Get all published files, find cbor error: {e}");
                    })
                    .ok()
            })
        )
    }
}

impl Store for RocksDb {
    // published file
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

    fn get_all_published_files(
        &self,
    ) -> Result<impl Iterator<Item = PublishedFileRecord> + Send, FileStoreError> {
        Ok(self.get_all_published_file_inner()?)
    }

    // downloading file
    fn add_downloading_file(&self, record: DownloadingFileRecord) -> Result<(), FileStoreError> {
        Ok(self.add_downloading_file_inner(record)?)
    }

    fn downloading_file_exists(&self, file_id: u64) -> Result<bool, FileStoreError> {
        Ok(self.downloading_file_exists_inner(file_id)?)
    }

    fn get_downloading_file(
        &self,
        file_id: u64,
    ) -> Result<Option<DownloadingFileRecord>, FileStoreError> {
        Ok(self.get_downloading_file_inner(file_id)?)
    }

    fn get_all_downloading_files(
        &self,
    ) -> Result<impl Iterator<Item = DownloadingFileRecord> + Send, FileStoreError> {
        Ok(self.get_all_downloading_file_inner()?)
    }
}
