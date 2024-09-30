use std::io::Read;
use std::io::Result;

use std::fs::File;
use std::fs::remove_dir_all;
use std::fs::create_dir_all;

use rusticodb::storage::config::Config;
use rusticodb::storage::os_inteface::BLOCK_SIZE;

pub fn create_tmp_test_folder() {
    let _ = create_dir_all(Config::data_folder());
}

pub fn destroy_tmp_test_folder() {
    let _ = remove_dir_all(Config::data_folder());
}

pub fn read_from_file(file_path: &str) -> Result<[u8; BLOCK_SIZE]> {
    let mut file = File::open(file_path)?;
    let mut content = [0; BLOCK_SIZE];
    file.read(&mut content)?;
    Ok(content)
}
