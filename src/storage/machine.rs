use crate::config::Config;
use crate::storage::os_interface::OsInterface;

#[derive(Debug)]
pub struct Machine { 
}

impl Machine {
    pub fn new() -> Self {
        Self {  }
    }

    pub fn create_database(&mut self, database_name: &String) {
        let database_folder = format!("{}/{}", Config::data_folder(), database_name);

        OsInterface::create_folder(&database_folder);
    }

    pub fn create_table(&mut self, database_name: &String, table_name: &String) {
        let table_filename = format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);

        OsInterface::create_file(&table_filename);
    }

}
