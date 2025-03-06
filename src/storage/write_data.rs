use crate::storage::OsInterface;
use crate::storage::BLOCK_SIZE;

pub fn write_data(page_key: &String, pos: u64, data: &[u8; BLOCK_SIZE]) {
    OsInterface::write_data(page_key, pos, data);
}

