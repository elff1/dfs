use std::{
    borrow::Cow,
    fs::{self, File},
    io,
    path::Path,
};

use crate::FileMetadata;

const FILE_METADATA_NAME: &str = "metadata.cbor";
const FILE_CHUNK_NAME: &str = "chunk_";

pub struct FsHelper();

impl FsHelper {
    pub async fn create_directory_async<P: AsRef<Path>>(
        directory: P,
        delete_first: bool,
    ) -> io::Result<()> {
        let directory = directory.as_ref();
        if delete_first {
            let _ = tokio::fs::remove_dir_all(directory).await;
        }
        tokio::fs::create_dir_all(directory).await
    }

    pub fn create_directory<P: AsRef<Path>>(directory: P, delete_first: bool) -> io::Result<()> {
        let directory = directory.as_ref();
        if delete_first {
            let _ = fs::remove_dir_all(directory);
        }
        fs::create_dir_all(directory)
    }

    pub fn create_file<P: AsRef<Path>>(file_path: P) -> io::Result<File> {
        File::create(file_path.as_ref())
    }

    pub async fn read_file_metadata_async<P: AsRef<Path>>(
        chunks_directory: P,
    ) -> io::Result<Vec<u8>> {
        tokio::fs::read(chunks_directory.as_ref().join(FILE_METADATA_NAME)).await
    }

    pub async fn write_file_metadata_async<P, C>(chunks_directory: P, contents: C) -> io::Result<()>
    where
        P: AsRef<Path>,
        C: AsRef<[u8]>,
    {
        tokio::fs::write(chunks_directory.as_ref().join(FILE_METADATA_NAME), contents).await
    }

    pub fn serde_write_file_metadata<P: AsRef<Path>>(
        chunks_directory: P,
        metadata: &FileMetadata,
    ) -> io::Result<()> {
        let cbor_file = File::create(chunks_directory.as_ref().join(FILE_METADATA_NAME))?;
        serde_cbor::to_writer(cbor_file, metadata).map_err(|e| io::Error::other(e.to_string()))?;

        Ok(())
    }

    /// 0: metadata, 1..n: chunks
    pub async fn read_file_chunk_async<P: AsRef<Path>>(
        chunks_directory: P,
        chunk_index: usize,
    ) -> io::Result<Vec<u8>> {
        let file_name = match chunk_index {
            0 => Cow::Borrowed(FILE_METADATA_NAME),
            _ => Cow::Owned(format!("{FILE_CHUNK_NAME}{chunk_index}")),
        };
        tokio::fs::read(chunks_directory.as_ref().join(file_name.as_ref())).await
    }

    /// 0: metadata, 1..n: chunks
    pub fn read_file_chunk<P: AsRef<Path>>(
        chunks_directory: P,
        chunk_index: usize,
    ) -> io::Result<Vec<u8>> {
        let file_name = match chunk_index {
            0 => Cow::Borrowed(FILE_METADATA_NAME),
            _ => Cow::Owned(format!("{FILE_CHUNK_NAME}{chunk_index}")),
        };
        fs::read(chunks_directory.as_ref().join(file_name.as_ref()))
    }

    pub async fn write_file_chunk_async<P, C>(
        chunks_directory: P,
        chunk_index: usize,
        contents: C,
    ) -> io::Result<()>
    where
        P: AsRef<Path>,
        C: AsRef<[u8]>,
    {
        tokio::fs::write(
            chunks_directory
                .as_ref()
                .join(format!("{FILE_CHUNK_NAME}{chunk_index}")),
            contents,
        )
        .await
    }

    pub fn write_file_chunk<P, C>(
        chunks_directory: P,
        chunk_index: usize,
        contents: C,
    ) -> io::Result<()>
    where
        P: AsRef<Path>,
        C: AsRef<[u8]>,
    {
        fs::write(
            chunks_directory
                .as_ref()
                .join(format!("{FILE_CHUNK_NAME}{chunk_index}")),
            contents,
        )
    }
}
