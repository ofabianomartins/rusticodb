use sqlparser::ast::ColumnDef;

use crate::config::Config;
use crate::storage::pager::Pager;
use crate::storage::os_interface::OsInterface;
use crate::storage::tuple::Tuple;
use crate::machine::column::Column;
use crate::machine::column::ColumnType;
use crate::machine::context::Context;
use crate::machine::result_set::ExecutionError;
use crate::machine::result_set::ResultSet;
use crate::machine::result_set::ResultSetType;
use crate::utils::logger::Logger;


#[derive(Debug)]
pub struct Machine { 
    pub pager: Pager,
    pub context: Context
}

impl Machine {
    pub fn new(pager: Pager, context: Context) -> Self {
        Self { pager, context }
    }

    pub fn database_exists(&mut self, database_name: &String) -> bool{
        return OsInterface::path_exists(&self.pager.format_database_name(database_name));
    }

    pub fn table_exists(&mut self, database_name: &String, table_name: &String) -> bool{
        return OsInterface::path_exists(&self.pager.format_table_name(database_name, table_name));
    }

    pub fn set_actual_database(&mut self, name: String) -> Result<ResultSet, ExecutionError> {
        if self.context.check_database_exists(&name) == false {
            return Err(ExecutionError::DatabaseNotExists(name));
        }
        self.context.set_actual_database(name);
        Ok(ResultSet::new_command(ResultSetType::Change, String::from("USE DATABASE")))
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

        let mut tuples: Vec<Tuple> = Vec::new();
        let mut tuple: Tuple = Tuple::new();
        tuple.push_string(&database_name);
        tuples.push(tuple);

        self.insert_tuples(&Config::system_database(), &Config::system_database_table_databases(), &mut tuples);

        Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE DATABASE")))
    }

    pub fn drop_database(&mut self, database_name: String, if_exists: bool) -> Result<ResultSet, ExecutionError>{
        if self.context.check_database_exists(&database_name) == false && if_exists {
            return Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP DATABASE")));
        }
        if self.context.check_database_exists(&database_name) == false {
            return Err(ExecutionError::DatabaseNotExists(database_name));
        }
        OsInterface::destroy_folder(&self.pager.format_database_name(&database_name));
        self.context.remove_database(database_name.to_string());

        let _ = self.remove_database(&database_name);

        Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP DATABASE")))
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

        let mut tuples: Vec<Tuple> = Vec::new();
        let mut tuple: Tuple = Tuple::new();
        tuple.push_string(&database_name);
        tuple.push_string(&table_name);
        tuples.push(tuple);

        self.insert_tuples(&Config::system_database(), &Config::system_database_table_tables(), &mut tuples);

        for column in columns.iter() {
            self.context.add_column(
                database_name.to_string(),
                table_name.to_string(),
                column.name.to_string(),
                ColumnType::Varchar
            );

            let mut tuples: Vec<Tuple> = Vec::new();
            let mut tuple: Tuple = Tuple::new();
            tuple.push_string(&database_name);
            tuple.push_string(&table_name);
            tuple.push_string(&column.name.to_string());
            tuples.push(tuple);

            self.insert_tuples(&Config::system_database(), &Config::system_database_table_columns(), &mut tuples);
        }
        Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE TABLE")))
    }

    pub fn drop_table(
        &mut self, 
        database_name: &String, 
        table_name: &String,
        if_exists: bool
    ) -> Result<ResultSet, ExecutionError>{
        if self.context.check_table_exists(&database_name, &table_name) == false && if_exists {
            return Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP TABLE")));
        }
        if self.context.check_table_exists(&database_name, &table_name) == false {
            return Err(ExecutionError::TableNotExists(database_name.to_string()));
        }
        OsInterface::destroy_file(&self.pager.format_table_name(database_name, table_name));
        self.context.remove_table(database_name.to_string(), table_name.to_string());

        Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP TABLE")))
    }

    pub fn remove_database(&mut self, name: &String) -> Result<ResultSet, ExecutionError>{
        let tuples = self.read_tuples(
            &Config::system_database(),
            &Config::system_database_table_databases()
        );

        let mut new_tuples: Vec<Tuple> = tuples
            .into_iter()
            .filter(|tuple| tuple.get_string(0).unwrap() != *name)
            .collect();

        self.update_tuples(
            &Config::system_database(),
            &Config::system_database_table_databases(),
            &mut new_tuples
        );

        Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP DATABASE")))
    }

    pub fn list_databases(&mut self) -> Result<ResultSet, ExecutionError>{
        let db_name = Config::system_database();
        let table_databases = Config::system_database_table_databases();
        let mut columns: Vec<Column> = Vec::new();
        columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));
        let tuples = self.read_tuples(&db_name, &table_databases);
        return Ok(ResultSet::new_select(columns, tuples))
    }

    pub fn list_tables(&mut self, db_name: String) -> Result<ResultSet, ExecutionError>{
        let mut columns: Vec<Column> = Vec::new();
        columns.push(Column::new_column(String::from("database"), ColumnType::Varchar));
        columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

        let tuples = self.read_tuples(
            &Config::system_database(),
            &Config::system_database_table_tables()
        ).into_iter()
            .filter(|tuple| tuple.get_string(0).unwrap() == db_name)
            .collect();

        return Ok(ResultSet::new_select(columns, tuples))
    }

    pub fn insert_tuples(&mut self, database_name: &String, table_name: &String, tuples: &mut Vec<Tuple>) {
        self.pager.insert_tuples(database_name, table_name, tuples);
        self.pager.flush_page(database_name, table_name);
    }

    pub fn update_tuples(&mut self, database_name: &String, table_name: &String, tuples: &mut Vec<Tuple>) {
        self.pager.update_tuples(database_name, table_name, tuples);
        self.pager.flush_page(database_name, table_name);
    }

    pub fn read_tuples(&mut self, database_name: &String, table_name: &String) -> Vec<Tuple> {
        Logger::debug(format!("Reading database {} table {}", database_name, table_name).leak());
        return self.pager.read_tuples(database_name, table_name)
    }
}
