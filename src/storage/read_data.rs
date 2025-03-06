use crate::storage::OsInterface;
use crate::storage::BLOCK_SIZE;

pub fn read_data(page_key: &String, pos: u64) -> [u8; BLOCK_SIZE] {
    if OsInterface::path_exists(page_key) {
         return OsInterface::read_data(page_key, pos);
    }
    let mut empty = [0; BLOCK_SIZE];
    empty[3] = 4u8;
    return empty;
}
