use bincode::serialize;
use bincode::deserialize;
use serde::Serialize;
use serde::Deserialize;

use crate::storage::BLOCK_SIZE;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Header {
    pub page_count: u64,
    pub next_rowid: u64,
}

pub fn header_new() -> Header {
    Header { page_count: 0, next_rowid: 1 }
}

pub fn header_serialize(header: &Header) -> [u8; BLOCK_SIZE] {
    let mut buffer = [0u8; BLOCK_SIZE];
    let serialized = serialize(&header).unwrap();

    buffer[..serialized.len()].copy_from_slice(&serialized);
    return buffer;
}

pub fn header_deserialize(header: &[u8; BLOCK_SIZE]) -> Header {
    return deserialize(header).unwrap();
}

pub fn header_get_next_rowid(header: &mut Header) -> u64 {
    let next_rowid = header.next_rowid;
    header.next_rowid += 1;

    return next_rowid;
}
