use std::path::Path;

use std::fs::create_dir;
use std::fs::remove_dir_all;
use std::fs::remove_file;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::utils::Logger;

pub const BLOCK_SIZE: usize = 4096;

#[derive(Debug)]
pub struct OsInterface { 
}

impl OsInterface {
    pub fn new() -> Self {
        Self {  }
    }

    pub fn path_exists(path_name: &String) -> bool {
        Logger::debug(format!("Check path {} exists", path_name).leak());

        return Path::new(&path_name).exists();
    }

    pub fn create_folder(folder_name: &String) {
        let _ = create_dir(folder_name);
    }

    pub fn destroy_folder(folder_name: &String) {
        let _ = remove_dir_all(folder_name);
    }

    pub fn create_folder_if_not_exists(folder_name: &String) {
        let _ = create_dir(folder_name);
    }

    pub fn create_file(file_name: &String) {
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
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(file_name)
            .unwrap();

        let _ = file.seek(SeekFrom::Start(pos));
        file.write(data).expect("buffer overflow");
    }

    pub fn read_data(file_name: &String, pos: u64) -> [u8; BLOCK_SIZE] {
        let mut buffer = [0u8; BLOCK_SIZE];

        let mut file = OpenOptions::new()
            .read(true)
            .open(file_name)
            .unwrap();

        let _ = file.seek(SeekFrom::Start(pos));
        match file.read(&mut buffer) {
            Ok(_) => buffer,
            Err(err) => { 
                println!("{:?}", err);
                buffer
            }
        }
    }
}
