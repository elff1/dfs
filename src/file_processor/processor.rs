use std::{
    mem::MaybeUninit,
    path::{Path, PathBuf},
};

use rs_merkle::{Hasher, MerkleTree, algorithms::Sha256};
use tokio::{
    fs::{self, File},
    io::{self, AsyncReadExt, AsyncWriteExt},
};

const LOG_TARGET: &str = "file_processor::processor";
const CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Debug)]
pub struct FileProcessorResult {
    pub original_file_name: String,
    pub number_of_chunks: u64,
    pub chunks_dirctory: PathBuf,
    pub merkle_root: [u8; 32],
    pub merkle_leaves: Vec<[u8; 32]>,
}

impl FileProcessorResult {
    pub fn new(
        original_file_name: String,
        number_of_chunks: u64,
        chunks_dirctory: PathBuf,
        merkle_root: [u8; 32],
        merkle_leaves: Vec<[u8; 32]>,
    ) -> Self {
        Self {
            original_file_name,
            number_of_chunks,
            chunks_dirctory,
            merkle_root,
            merkle_leaves,
        }
    }
}

pub struct FileProcessor();

//#[tonic::async_trait]
impl FileProcessor {
    pub async fn publish_file<P: AsRef<Path>>(file: P) -> io::Result<FileProcessorResult> {
        let file = file.as_ref();
        log::debug!(target: LOG_TARGET, "Start publish: {}", file.display());

        if fs::metadata(file).await?.is_dir() {
            todo!();
        } else {
            publish_one_file(file).await
        }
    }
}

async fn publish_one_file(file_path: &Path) -> io::Result<FileProcessorResult> {
    let mut components = file_path.components();
    let file_name = components
        .next_back()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .ok_or(io::Error::new(
            std::io::ErrorKind::InvalidFilename,
            file_path.to_string_lossy(),
        ))?;
    let mut chunk_dir = components.as_path().to_path_buf();
    chunk_dir.push(format!("chunks_{}", file_name.replace(".", "_")));
    fs::remove_dir_all(&chunk_dir).await?;
    fs::create_dir_all(&chunk_dir).await?;

    log::info!(target: LOG_TARGET, "Chunk dir: {}", chunk_dir.display());

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

        // Why does `fs::write(&chunk_path, &buf[..buf_offset]).await?;` turn `buf` into `OwnedBuf` first, while `File::write_all()` doesn't?
        let mut chunk_file = File::create(chunk_dir.join(format!("chunk_{chunk_index}"))).await?;
        chunk_file.write_all(&buf[..buf_offset]).await?;
        chunk_file.flush().await?;

        buf_offset = 0;
    }

    let merkle_root = MerkleTree::<Sha256>::from_leaves(&merkle_leaves)
        .root()
        .ok_or(io::Error::other("can not get Merkle root"))?;

    Ok(FileProcessorResult::new(
        file_name,
        chunk_index,
        chunk_dir,
        merkle_root,
        merkle_leaves,
    ))
}
