use std::path::Path;

use std::fs::create_dir;
use std::fs::remove_dir_all;
use std::fs::remove_file;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::utils::Logger;

pub const BLOCK_SIZE: usize = 4096;

pub fn path_exists(path_name: &String) -> bool {
    Logger::debug(format!("Check path {} exists", path_name).leak());

    return Path::new(&path_name).exists();
}

pub fn create_folder(folder_name: &String) {
    Logger::debug(format!("Creating folder {}", folder_name).leak());
    let _ = create_dir(folder_name);
}

pub fn destroy_folder(folder_name: &String) {
    Logger::debug(format!("Destroy folder {}", folder_name).leak());
    let _ = remove_dir_all(folder_name);
}

pub fn create_folder_if_not_exists(folder_name: &String) {
    let _ = create_dir(folder_name);
}

pub fn create_file(file_name: &String) {
    Logger::debug(format!("Creating file {}", file_name).leak());
    let _file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(file_name)
        .unwrap();
}

pub fn destroy_file(file_name: &String) {
    let _ = remove_file(file_name);
}

pub fn write_data(file_name: &String, pos: u64, data: &[u8; BLOCK_SIZE]) {
    Logger::debug(format!("write data {} on position {}", file_name, pos).leak());
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(file_name)
        .unwrap();

    let _ = file.seek(SeekFrom::Start(pos*BLOCK_SIZE as u64));
    file.write(data).expect("buffer overflow");
}

pub fn read_data(file_name: &String, pos: u64) -> [u8; BLOCK_SIZE] {
    Logger::debug(format!("read data {} on position {}", file_name, pos).leak());
    let mut buffer = [0u8; BLOCK_SIZE];
    buffer[8] = 1u8;

    let mut file = OpenOptions::new()
        .read(true)
        .open(file_name)
        .unwrap();

    let _ = file.seek(SeekFrom::Start(pos*BLOCK_SIZE as u64));
    match file.read(&mut buffer) {
        Ok(_) => buffer,
        Err(err) => { 
            println!("{:?}", err);
            buffer
        }
    }
}
