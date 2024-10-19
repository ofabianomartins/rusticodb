use crate::storage::pager::Pager;
use crate::storage::os_interface::OsInterface;
use crate::storage::tuple::Tuple;

#[derive(Debug)]
pub struct Machine { 
    pager: Pager
}

impl Machine {
    pub fn new(pager: Pager) -> Self {
        Self { pager }
    }

    pub fn database_exists(&mut self, database_name: &String) -> bool{
        return OsInterface::path_exists(&self.pager.format_database_name(database_name));
    }

    pub fn table_exists(&mut self, database_name: &String, table_name: &String) -> bool{
        return OsInterface::path_exists(&self.pager.format_table_name(database_name, table_name));
    }

    pub fn create_database(&mut self, database_name: &String) {
        OsInterface::create_folder(&self.pager.format_database_name(database_name));
    }

    pub fn create_table(&mut self, database_name: &String, table_name: &String) {
        OsInterface::create_file(&self.pager.format_table_name(database_name, table_name));
    }

    pub fn insert_tuples(&mut self, database_name: &String, table_name: &String, tuples: &mut Vec<Tuple>) {
        self.pager.insert_tuples(database_name, table_name, tuples);
    }

    pub fn read_tuples(&mut self, database_name: &String, table_name: &String) -> Vec<Tuple> {
        return self.pager.read_tuples(database_name, table_name)
    }


}
