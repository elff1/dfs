use std::{io, path::Path};

use tokio::fs;

use super::PROCESSING_RESULT_FILE_NAME;

pub struct FsHelper;

impl FsHelper {
    pub async fn create_directory<P: AsRef<Path>>(directory: P) -> io::Result<()> {
        let directory = directory.as_ref();
        let _ = fs::remove_dir_all(directory).await;
        fs::create_dir_all(directory).await
    }

    pub async fn read_file_metadata<P: AsRef<Path>>(chunks_directory: P) -> io::Result<Vec<u8>> {
        fs::read(chunks_directory.as_ref().join(PROCESSING_RESULT_FILE_NAME)).await
    }

    pub fn read_file_chunk<P: AsRef<Path>>(
        chunks_directory: P,
        chunk_index: usize,
    ) -> io::Result<Vec<u8>> {
        std::fs::read(
            chunks_directory
                .as_ref()
                .join(format!("chunk_{chunk_index}")),
        )
    }

    pub fn write_file_metadata<P: AsRef<Path>>(
        chunks_directory: P,
        contents: &[u8],
    ) -> io::Result<()> {
        std::fs::write(
            chunks_directory.as_ref().join(PROCESSING_RESULT_FILE_NAME),
            contents,
        )
    }
}
