use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishedFile {
    pub number_of_chunks: u64,
    pub merkle_root: [u8; 32],
}

impl PublishedFile {
    pub fn new(number_of_chunks: u64, merkle_root: [u8; 32]) -> Self {
        Self {
            number_of_chunks,
            merkle_root,
        }
    }
}
