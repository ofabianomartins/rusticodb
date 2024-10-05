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
        return OsInterface::path_exists(&self.format_database_name(database_name));
    }

    pub fn table_exists(&mut self, database_name: &String, table_name: &String) -> bool{
        return OsInterface::path_exists(&self.format_table_name(database_name, table_name));
    }

    pub fn create_database(&mut self, database_name: &String) {
        OsInterface::create_folder(&self.format_database_name(database_name));
    }

    pub fn create_table(&mut self, database_name: &String, table_name: &String) {
        OsInterface::create_file(&self.format_table_name(database_name, table_name));
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

    fn format_database_name(&mut self, database_name: &String) -> String{
        return format!("{}/{}", Config::data_folder(), database_name);
    }

    fn format_table_name(&mut self, database_name: &String, table_name: &String) -> String{
        return format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);
    }

}
