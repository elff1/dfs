use std::hash::{Hash, Hasher};

use rs_sha256::Sha256Hasher;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Hash64(u64);
pub type Hash256 = [u8; 32];

impl Hash64 {
    pub fn new(hash: u64) -> Self {
        Self(hash)
    }

    pub fn to_array(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl<T: Hash> From<&T> for Hash64 {
    fn from(value: &T) -> Self {
        let mut hasher = Sha256Hasher::default();
        value.hash(&mut hasher);
        Self(hasher.finish())
    }
}

impl TryFrom<&[u8]> for Hash64 {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let value: [u8; 8] = value.try_into().map_err(|_| {})?;
        Ok(Self(u64::from_be_bytes(value)))
    }
}

impl From<Hash64> for Vec<u8> {
    fn from(value: Hash64) -> Self {
        value.to_array().to_vec()
    }
}

impl std::fmt::Display for Hash64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.0))
    }
}

pub type FileId = Hash64;

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
    pub chunk_hashes: Vec<Hash64>,
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
            chunk_hashes: vec![],
            merkle_root,
            merkle_leaves,
            public,
        });

        metadata.file_id = (&metadata).into();
        metadata.chunk_hashes = metadata
            .merkle_leaves
            .iter()
            .map(|merkle_hash| merkle_hash.into())
            .collect();

        metadata
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
