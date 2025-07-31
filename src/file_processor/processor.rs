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
    io::{self, AsyncReadExt, AsyncWriteExt},
};

const LOG_TARGET: &str = "file_processor::processor";
const CHUNK_SIZE: usize = 1024 * 1024;
const PROCESSING_RESULT_FILE_NAME: &str = "metadata.cbor";

#[derive(Debug, Serialize, Deserialize)]
pub struct FileProcessResult {
    pub original_file_name: String,
    pub number_of_chunks: u64,
    pub chunks_directory: PathBuf,
    pub merkle_root: [u8; 32],
    pub merkle_leaves: Vec<[u8; 32]>,
    pub public: bool,
}

impl FileProcessResult {
    pub fn new(
        original_file_name: String,
        number_of_chunks: u64,
        chunks_directory: PathBuf,
        merkle_root: [u8; 32],
        merkle_leaves: Vec<[u8; 32]>,
        public: bool,
    ) -> Self {
        Self {
            original_file_name,
            number_of_chunks,
            chunks_directory,
            merkle_root,
            merkle_leaves,
            public,
        }
    }

    pub fn hash_sha256(&self) -> FileProcessResultHash {
        let mut hasher = Sha256Hasher::default();
        self.hash(&mut hasher);
        FileProcessResultHash(hasher.finish())
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

impl TryFrom<Vec<u8>> for FileProcessResult {
    type Error = serde_cbor::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(&value)
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct FileProcessResultHash(u64);

impl FileProcessResultHash {
    pub fn new(hash: u64) -> Self {
        Self(hash)
    }

    pub fn raw(&self) -> u64 {
        self.0
    }

    pub fn to_array(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl TryFrom<&[u8]> for FileProcessResultHash {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let value: [u8; 8] = value.try_into().map_err(|_| {})?;
        Ok(Self(u64::from_be_bytes(value)))
    }
}

impl From<FileProcessResultHash> for Vec<u8> {
    fn from(value: FileProcessResultHash) -> Self {
        value.to_array().to_vec()
    }
}

pub struct FileProcessor();

//#[tonic::async_trait]
impl FileProcessor {
    pub async fn process_file(file_path: String, public: bool) -> io::Result<FileProcessResult> {
        let file = PathBuf::from(file_path);

        log::debug!(target: LOG_TARGET, "Start publish: {}", file.display());

        if fs::metadata(&file).await?.is_dir() {
            todo!();
        } else {
            process_one_file(&file, public).await
        }
    }

    pub async fn get_file_metadata<P: AsRef<Path>>(chunks_directory: P) -> io::Result<Vec<u8>> {
        fs::read(chunks_directory.as_ref().join(PROCESSING_RESULT_FILE_NAME)).await
    }
}

async fn process_one_file(file_path: &Path, public: bool) -> io::Result<FileProcessResult> {
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
    fs::remove_dir_all(&chunk_dir).await.unwrap_or_default();
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

    let result = FileProcessResult::new(
        file_name,
        chunk_index,
        chunk_dir,
        merkle_root,
        merkle_leaves,
        public,
    );

    let cbor_file = fs::File::create(result.chunks_directory.join(PROCESSING_RESULT_FILE_NAME))
        .await?
        .into_std()
        .await;
    serde_cbor::to_writer(cbor_file, &result).map_err(|e| io::Error::other(e.to_string()))?;

    Ok(result)
}
