use std::fs::File;
use std::fs::create_dir;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::storage::config::Config;

pub const BLOCK_SIZE: usize = 4096;

#[derive(Debug)]
pub struct Page { 

}

#[derive(Debug)]
pub struct Pager { 
}

impl Pager {
    pub fn new() -> Self {
        Self {  }
    }

    pub fn create_database(&mut self, database_name: &String) {
        let database_folder = format!("{}/{}", Config::data_folder(), database_name);

        let _ = create_dir(database_folder);
    }

    pub fn create_file(&mut self, database_name: &String, table_name: &String) {
        let table_filename = format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);

        let _file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(table_filename)
            .unwrap();
    }

    pub fn write_data(&mut self, database_name: &String, table_name: &String, pos: u64, data: &[u8; BLOCK_SIZE]) {
        let rows_filename = format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);

        let mut file = OpenOptions::new()
            .write(true)
            .open(rows_filename)
            .unwrap();

        let _ = file.seek(SeekFrom::Start(pos));
        file.write(data).expect("buffer overflow");
    }

    pub fn read_data(&mut self, database_name: &String, table_name: &String, pos: u64) -> [u8; BLOCK_SIZE] {
        let rows_filename = format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);
        let mut buffer = [0u8; BLOCK_SIZE];

        let mut file = OpenOptions::new()
            .read(true)
            .open(rows_filename)
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
