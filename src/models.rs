use std::hash::{Hash, Hasher};

use rs_sha256::Sha256Hasher;
use serde::{Deserialize, Serialize};

pub type Hash64 = u64;
pub type Hash256 = [u8; 32];

#[derive(Debug, Default, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct FileId(Hash64);

impl FileId {
    pub fn new(hash: Hash64) -> Self {
        Self(hash)
    }

    pub fn to_array(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl TryFrom<&[u8]> for FileId {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let value: [u8; 8] = value.try_into().map_err(|_| {})?;
        Ok(Self(Hash64::from_be_bytes(value)))
    }
}

impl From<FileId> for Vec<u8> {
    fn from(value: FileId) -> Self {
        value.to_array().to_vec()
    }
}

impl std::fmt::Display for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.0))
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct FileChunkId {
    pub(crate) file_id: FileId,
    /// 0: metadata, 1..n: chunks
    pub(crate) chunk_index: usize,
}

impl FileChunkId {
    pub fn new(file_id: FileId, chunk_index: usize) -> Self {
        Self {
            file_id,
            chunk_index,
        }
    }
}

impl From<&FileChunkId> for Vec<u8> {
    fn from(value: &FileChunkId) -> Self {
        serde_cbor::to_vec(value)
            .map_err(|e| {
                log::error!("serde_cbor::to_vec(FileChunkId[{value}]) failed: {e}");
            })
            .unwrap()
    }
}

impl TryFrom<&[u8]> for FileChunkId {
    type Error = serde_cbor::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(value)
    }
}

impl std::fmt::Display for FileChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}/{}", self.file_id.0, self.chunk_index))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileMetadata {
    pub file_id: FileId,
    pub original_file_name: String,
    pub file_length: u64,
    pub number_of_chunks: u32,
    pub merkle_root: Hash256,
    pub merkle_leaves: Vec<Hash256>,
    pub public: bool,
}

impl FileMetadata {
    pub fn new(
        original_file_name: String,
        file_length: u64,
        number_of_chunks: u32,
        merkle_root: Hash256,
        merkle_leaves: Vec<Hash256>,
        public: bool,
    ) -> Box<Self> {
        let mut metadata = Box::new(Self {
            file_id: FileId::default(),
            original_file_name,
            file_length,
            number_of_chunks,
            merkle_root,
            merkle_leaves,
            public,
        });

        metadata.file_id = metadata.hash_sha256();

        metadata
    }

    fn hash_sha256(&self) -> FileId {
        let mut hasher = Sha256Hasher::default();
        self.hash(&mut hasher);
        FileId::new(hasher.finish())
    }
}

impl Hash for FileMetadata {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.original_file_name.hash(state);
        self.number_of_chunks.hash(state);
        self.merkle_root.hash(state);
        self.public.hash(state);
    }
}

impl TryFrom<&[u8]> for Box<FileMetadata> {
    type Error = serde_cbor::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(value)
    }
}
