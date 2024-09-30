use std::fs::create_dir;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

pub const BLOCK_SIZE: usize = 4096;

#[derive(Debug)]
pub struct OsInterface { 
}

impl OsInterface {
    pub fn new() -> Self {
        Self {  }
    }

    pub fn create_folder(folder_name: &String) {
        let _ = create_dir(folder_name);
    }

    pub fn create_file(file_name: &String) {
        let _file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(file_name)
            .unwrap();
    }

    pub fn write_data(file_name: &String, pos: u64, data: &[u8; BLOCK_SIZE]) {

        let mut file = OpenOptions::new()
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
                println!("{}", err);
                buffer
            }
        }
    }
}
