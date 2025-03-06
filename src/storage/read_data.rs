use crate::storage::BLOCK_SIZE;
use crate::storage::os_interface::read_data as read_data_storage;
use crate::storage::os_interface::path_exists;

pub fn read_data(page_key: &String, pos: u64) -> [u8; BLOCK_SIZE] {
    if path_exists(page_key) {
        return read_data_storage(page_key, pos);
    }
    let mut empty = [0; BLOCK_SIZE];
    empty[3] = 4u8;
    return empty;
}
