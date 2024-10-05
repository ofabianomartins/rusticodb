use crate::config::Config;
use crate::storage::pager::Pager;
use crate::storage::os_interface::BLOCK_SIZE;
use crate::storage::os_interface::OsInterface;
use crate::storage::tuple::Tuple;

#[derive(Debug)]
pub struct Machine { 
}

impl Machine {
    pub fn new() -> Self {
        Self {  }
    }

    pub fn database_exists(&mut self, database_name: &String) -> bool{
        let database_folder = format!("{}/{}", Config::data_folder(), database_name);

        return OsInterface::path_exists(&database_folder);
    }

    pub fn table_exists(&mut self, database_name: &String, table_name: &String) -> bool{
        let table_filename = format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);

        return OsInterface::path_exists(&table_filename);
    }

    pub fn create_database(&mut self, database_name: &String) {
        let database_folder = format!("{}/{}", Config::data_folder(), database_name);

        OsInterface::create_folder(&database_folder);
    }

    pub fn create_table(&mut self, database_name: &String, table_name: &String) {
        let table_filename = format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);

        OsInterface::create_file(&table_filename);
    }

    pub fn insert_tuples(&mut self, database_name: &String, table_name: &String, tuples: &mut Vec<Tuple>) {
        let mut pager = Pager::new();

        let mut buffer: Vec<u8> = Vec::new();
        let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];

        let size = tuples.len() as u64;  
        buffer.append(&mut size.to_le_bytes().to_vec());

        for tuple in tuples {
            let size = tuple.cells.len() as u64;  
            buffer.append(&mut size.to_le_bytes().to_vec());

            for cell in &mut tuple.cells {
                buffer.append(&mut cell.data);
            }
        }

        for (idx, elem) in &mut buffer.iter().enumerate() {
            raw_buffer[idx] = *elem;
        }

        pager.write_data(database_name, table_name, 0, &raw_buffer);
    }

}
