use std::{
    fs,
    io::{self, BufWriter, Read, Write},
    mem::MaybeUninit,
    path::{Path, PathBuf},
};

use rs_merkle::{Hasher, MerkleTree, algorithms::Sha256};

use super::FsHelper;
use crate::FileMetadata;

const LOG_TARGET: &str = "app::fs::processor";
/// chunk size 1MB
const CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Debug)]
pub(super) struct FileSplitResult {
    pub metadata: Box<FileMetadata>,
    pub chunks_directory: PathBuf,
}

pub(super) struct FileProcessor();

impl FileProcessor {
    pub fn split_file<P: AsRef<Path>>(file_path: P, public: bool) -> io::Result<FileSplitResult> {
        let file_path = file_path.as_ref();
        let file_path_display = file_path.display();

        log::info!(target: LOG_TARGET, "Start process file[{file_path_display}] before publish");

        let process_result = if fs::metadata(file_path)?.is_dir() {
            todo!();
        } else {
            split_one_file(file_path, public)?
        };

        log::info!(target: LOG_TARGET,
            "Finish process file[{file_path_display}] before publish: file_id[{}]",
            process_result.metadata.file_id
        );

        Ok(process_result)
    }

    pub fn merge_file<P: AsRef<Path>>(
        chunks_directory: P,
        original_file_name: &str,
        number_of_chunks: u32,
    ) -> io::Result<()> {
        let chunks_directory = chunks_directory.as_ref();
        let mut file_path = chunks_directory
            .parent()
            .ok_or(io::Error::new(
                io::ErrorKind::InvalidInput,
                chunks_directory.to_string_lossy(),
            ))?
            .to_path_buf();
        file_path.push(original_file_name);

        let mut file_writter = BufWriter::new(FsHelper::create_file(&file_path)?);
        for chunk_index in 1..=number_of_chunks as usize {
            let chunk = FsHelper::read_file_chunk(chunks_directory, chunk_index)?;
            file_writter.write_all(chunk.as_slice())?;
        }
        file_writter.flush()?;

        Ok(())
    }
}

fn split_one_file(file_path: &Path, public: bool) -> io::Result<FileSplitResult> {
    let mut components = file_path.components();
    let file_name = components
        .next_back()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .ok_or(io::Error::new(
            io::ErrorKind::InvalidFilename,
            file_path.to_string_lossy(),
        ))?;
    let mut chunks_directory = components.as_path().to_path_buf();
    chunks_directory.push(format!("chunks_{}", file_name.replace(".", "_")));
    // Do not delete the directory
    FsHelper::create_directory(&chunks_directory, false)?;

    log::info!(target: LOG_TARGET, "Chunks directory: {}", chunks_directory.display());

    let mut file = fs::File::open(file_path)?;
    let mut file_length = 0;
    let mut chunk_index = 0;
    let mut merkle_leaves = vec![];

    // Use MaybeUninit to avoid initializing the buffer
    let mut buf: Vec<MaybeUninit<u8>> = Vec::with_capacity(CHUNK_SIZE);
    // SAFETY: We will be writing to the buffer before reading from it.
    unsafe {
        buf.set_len(CHUNK_SIZE);
    }
    let buf = &mut buf[..] as *mut [MaybeUninit<u8>] as *mut [u8];
    let buf = unsafe { &mut *buf };
    let mut buf_offset = 0;

    loop {
        let read_len = file.read(&mut buf[buf_offset..])?;

        if read_len > 0 {
            buf_offset += read_len;
            if buf_offset < CHUNK_SIZE {
                continue;
            }
        } else if buf_offset == 0 {
            break;
        }

        file_length += buf_offset as u64;
        chunk_index += 1;
        merkle_leaves.push(Sha256::hash(&buf[..buf_offset]));

        FsHelper::write_file_chunk(&chunks_directory, chunk_index as usize, &buf[..buf_offset])?;

        buf_offset = 0;
    }

    let merkle_root = MerkleTree::<Sha256>::from_leaves(&merkle_leaves)
        .root()
        .ok_or(io::Error::other("can not get Merkle root"))?;

    let result = FileSplitResult {
        metadata: FileMetadata::new(
            file_name,
            file_length,
            chunk_index,
            merkle_root,
            merkle_leaves,
            public,
        ),
        chunks_directory,
    };

    FsHelper::serde_write_file_metadata(&result.chunks_directory, &result.metadata)?;

    Ok(result)
}
