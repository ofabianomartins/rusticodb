use crate::storage::BLOCK_SIZE;
use crate::storage::os_interface::write_data as write_data_storage;

pub fn write_data(page_key: &String, pos: u64, data: &[u8; BLOCK_SIZE]) {
    write_data_storage(page_key, pos, data);
}

