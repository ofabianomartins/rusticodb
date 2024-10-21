
use crate::storage::pager::Pager;
use crate::storage::os_interface::OsInterface;
use crate::storage::tuple::Tuple;
use crate::machine::context::Context;
use crate::machine::result_set::ExecutionError;
use crate::machine::result_set::ResultSet;

#[derive(Debug)]
pub struct Machine { 
    pager: Pager,
    pub context: Context
}

impl Machine {
    pub fn new(pager: Pager, context: Context) -> Self {
        Self { pager, context }
    }

    pub fn set_actual_database(&mut self, name: String) -> Result<ResultSet, ExecutionError> {
        if self.context.check_database_exists(&name) == false {
            return Err(ExecutionError::DatabaseNotExists(name));
        }
        self.context.set_actual_database(name);
        Ok(ResultSet {})
    }

    pub fn database_exists(&mut self, database_name: &String) -> bool{
        return OsInterface::path_exists(&self.pager.format_database_name(database_name));
    }

    pub fn table_exists(&mut self, database_name: &String, table_name: &String) -> bool{
        return OsInterface::path_exists(&self.pager.format_table_name(database_name, table_name));
    }

    pub fn create_database(&mut self, database_name: String) -> Result<ResultSet, ExecutionError>{
        if self.context.check_database_exists(&database_name) {
            return Err(ExecutionError::DatabaseExists(database_name));
        }
        OsInterface::create_folder(&self.pager.format_database_name(&database_name));
        self.context.add_database(database_name.to_string());
        Ok(ResultSet {})
    }

    pub fn create_table(&mut self, database_name: &String, table_name: &String) {
        OsInterface::create_file(&self.pager.format_table_name(database_name, table_name));
        self.context.add_table(database_name.to_string(), table_name.to_string());
    }

    pub fn insert_tuples(&mut self, database_name: &String, table_name: &String, tuples: &mut Vec<Tuple>) {
        self.pager.insert_tuples(database_name, table_name, tuples);
    }

    pub fn read_tuples(&mut self, database_name: &String, table_name: &String) -> Vec<Tuple> {
        return self.pager.read_tuples(database_name, table_name)
    }
}
