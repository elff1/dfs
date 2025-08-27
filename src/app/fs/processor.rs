use std::{
    hash::{Hash, Hasher as _},
    mem::MaybeUninit,
    path::{Path, PathBuf},
};

use rs_merkle::{Hasher, MerkleTree, algorithms::Sha256};
use rs_sha256::Sha256Hasher;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File},
    io::{self, AsyncReadExt},
};

use super::FsHelper;
use crate::FileId;

const LOG_TARGET: &str = "app::fs::processor";
/// chunk size 1MB
const CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub struct FileProcessResult {
    pub original_file_name: String,
    pub number_of_chunks: u32,
    pub chunks_directory: PathBuf,
    pub merkle_root: [u8; 32],
    pub merkle_leaves: Vec<[u8; 32]>,
    pub public: bool,
}

impl FileProcessResult {
    pub fn new(
        original_file_name: String,
        number_of_chunks: u32,
        chunks_directory: PathBuf,
        merkle_root: [u8; 32],
        merkle_leaves: Vec<[u8; 32]>,
        public: bool,
    ) -> Box<Self> {
        Box::new(Self {
            original_file_name,
            number_of_chunks,
            chunks_directory,
            merkle_root,
            merkle_leaves,
            public,
        })
    }

    pub fn hash_sha256(&self) -> FileId {
        let mut hasher = Sha256Hasher::default();
        self.hash(&mut hasher);
        FileId::new(hasher.finish())
    }
}

impl Hash for FileProcessResult {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.original_file_name.hash(state);
        self.number_of_chunks.hash(state);
        self.merkle_root.hash(state);
        self.public.hash(state);
    }
}

impl TryFrom<&[u8]> for Box<FileProcessResult> {
    type Error = serde_cbor::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(value)
    }
}

pub struct FileProcessor();

impl FileProcessor {
    pub async fn process_file(
        file_path: String,
        public: bool,
    ) -> io::Result<Box<FileProcessResult>> {
        let file = PathBuf::from(file_path);

        log::debug!(target: LOG_TARGET, "Start publish: {}", file.display());

        if fs::metadata(&file).await?.is_dir() {
            todo!();
        } else {
            process_one_file(&file, public).await
        }
    }
}

async fn process_one_file(file_path: &Path, public: bool) -> io::Result<Box<FileProcessResult>> {
    let mut components = file_path.components();
    let file_name = components
        .next_back()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .ok_or(io::Error::new(
            std::io::ErrorKind::InvalidFilename,
            file_path.to_string_lossy(),
        ))?;
    let mut chunks_directory = components.as_path().to_path_buf();
    chunks_directory.push(format!("chunks_{}", file_name.replace(".", "_")));
    // Do not delete the directory
    FsHelper::create_directory_async(&chunks_directory, false).await?;

    log::info!(target: LOG_TARGET, "Chunks directory: {}", chunks_directory.display());

    let mut file = File::open(file_path).await?;
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
        let read_len = file.read(&mut buf[buf_offset..]).await?;

        if read_len > 0 {
            buf_offset += read_len;
            if buf_offset < CHUNK_SIZE {
                continue;
            }
        } else if buf_offset == 0 {
            break;
        }

        chunk_index += 1;
        merkle_leaves.push(Sha256::hash(&buf[..buf_offset]));

        FsHelper::write_file_chunk(&chunks_directory, chunk_index as usize, &buf[..buf_offset])?;

        buf_offset = 0;
    }

    let merkle_root = MerkleTree::<Sha256>::from_leaves(&merkle_leaves)
        .root()
        .ok_or(io::Error::other("can not get Merkle root"))?;

    let result = FileProcessResult::new(
        file_name,
        chunk_index,
        chunks_directory,
        merkle_root,
        merkle_leaves,
        public,
    );

    FsHelper::serde_write_file_metadata(&result.chunks_directory, &result)?;

    Ok(result)
}
