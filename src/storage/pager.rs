use crate::config::Config;
use crate::storage::os_interface::OsInterface;
use crate::storage::os_interface::BLOCK_SIZE;

#[derive(Debug)]
pub struct Pager { 
}

impl Pager {
    pub fn new() -> Self {
        Self {  }
    }

    pub fn write_data(&mut self, database_name: &String, table_name: &String, pos: u64, data: &[u8; BLOCK_SIZE]) {
        let rows_filename = format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);

        OsInterface::write_data(&rows_filename, pos, data);
    }

    pub fn read_data(&mut self, database_name: &String, table_name: &String, pos: u64) -> [u8; BLOCK_SIZE] {
        let rows_filename = format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);

        return OsInterface::read_data(&rows_filename, pos);
    }
}
