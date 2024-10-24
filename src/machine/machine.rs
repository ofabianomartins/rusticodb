use sqlparser::ast::ColumnDef;

use crate::storage::pager::Pager;
use crate::storage::os_interface::OsInterface;
use crate::storage::tuple::Tuple;
use crate::machine::context::Context;
use crate::machine::result_set::ExecutionError;
use crate::machine::result_set::ResultSet;

use super::result_set::ResultSetType;

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
        Ok(ResultSet::new_command(ResultSetType::Change, String::from("USE DATABASE")))
    }

    pub fn database_exists(&mut self, database_name: &String) -> bool{
        return OsInterface::path_exists(&self.pager.format_database_name(database_name));
    }

    pub fn table_exists(&mut self, database_name: &String, table_name: &String) -> bool{
        return OsInterface::path_exists(&self.pager.format_table_name(database_name, table_name));
    }

    pub fn create_database(
        &mut self, 
        database_name: String,
        if_not_exists: bool
    ) -> Result<ResultSet, ExecutionError>{
        if self.context.check_database_exists(&database_name) && if_not_exists {
            return Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE DATABASE")));
        }
        if self.context.check_database_exists(&database_name) {
            return Err(ExecutionError::DatabaseExists(database_name));
        }
        OsInterface::create_folder(&self.pager.format_database_name(&database_name));
        self.context.add_database(database_name.to_string());
        Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE DATABASE")))
    }

    pub fn create_table(
        &mut self, 
        database_name: &String, 
        table_name: &String,
        if_not_exists: bool,
        columns: Vec<ColumnDef>
    ) -> Result<ResultSet, ExecutionError>{
        if self.context.check_table_exists(&database_name, &table_name) && if_not_exists {
            return Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE TABLE")));
        }
        if self.context.check_table_exists(&database_name, &table_name) {
            return Err(ExecutionError::DatabaseExists(database_name.to_string()));
        }
        OsInterface::create_file(&self.pager.format_table_name(database_name, table_name));
        self.context.add_table(database_name.to_string(), table_name.to_string());

        for column in columns.iter() {
            self.context.add_column(
                database_name.to_string(),
                table_name.to_string(),
                column.name.to_string()
            );
        }
        Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE TABLE")))
    }

    pub fn insert_tuples(&mut self, database_name: &String, table_name: &String, tuples: &mut Vec<Tuple>) {
        self.pager.insert_tuples(database_name, table_name, tuples);
    }

    pub fn read_tuples(&mut self, database_name: &String, table_name: &String) -> Vec<Tuple> {
        return self.pager.read_tuples(database_name, table_name)
    }
}
