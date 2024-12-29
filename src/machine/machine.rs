use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOption;
use sqlparser::ast::DataType;
use sqlparser::ast::SequenceOptions;

use crate::config::Config;
use crate::storage::pager::Pager;
use crate::storage::os_interface::OsInterface;
use crate::storage::tuple::Tuple;
use crate::machine::table::Table;
use crate::machine::column::Column;
use crate::machine::column::ColumnType;
use crate::machine::context::Context;
use crate::machine::result_set::ResultSet;
use crate::machine::result_set::ResultSetType;
use crate::utils::logger::Logger;
use crate::utils::execution_error::ExecutionError;

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
        tuple.push_unsigned_bigint(1u64);
        tuple.push_string(&database_name);
        tuples.push(tuple);

        let table = Table::new(Config::system_database(), Config::system_database_table_databases());

        self.insert_tuples(&table, &mut tuples);

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
        let table = Table::new(database_name.clone(), table_name.clone());
        if self.context.check_table_exists(&table) && if_not_exists {
            return Ok(
                ResultSet::new_command(
                    ResultSetType::Change, String::from("CREATE TABLE")
                )
            );
        }
        if self.context.check_table_exists(&table) {
            return Err(ExecutionError::DatabaseExists(database_name.to_string()));
        }
        OsInterface::create_file(
            &self.pager.format_table_name(database_name, table_name)
        );
        self.context.add_table(database_name.to_string(), table_name.to_string());

        let mut tuples: Vec<Tuple> = Vec::new();
        let mut tuple: Tuple = Tuple::new();
        tuple.push_unsigned_bigint(1u64);
        tuple.push_string(&database_name);
        tuple.push_string(&table_name);
        tuples.push(tuple);

        let table = Table::new(
            Config::system_database(),
            Config::system_database_table_tables()
        );

        self.insert_tuples(&table, &mut tuples);

        for column in columns.iter() {
            self.context.add_column(
                database_name.to_string(),
                table_name.to_string(),
                column.name.to_string(),
                ColumnType::Varchar
            );

            let mut type_column: String = String::from("");
            let mut notnull_column: bool = false;
            let mut unique_column: bool = false;
            let mut primary_key_column: bool = false;

            match column.data_type {
                DataType::BigInt(None) => { type_column = String::from("BIGINT") },
                DataType::Integer(None) => { type_column = String::from("INTEGER") },
                DataType::Varchar(None) => { type_column = String::from("VARCHAR") }
                _ => {}
            }

            for option in &column.options {
                match option.option {
                    ColumnOption::NotNull => { notnull_column = true }
                    ColumnOption::Unique { is_primary, ..} => {
                        notnull_column = true;
                        unique_column = true;
                        primary_key_column = is_primary;
                    }
                    _ => {}
                }
            }

            let mut tuples: Vec<Tuple> = Vec::new();
            let mut tuple: Tuple = Tuple::new();
            tuple.push_unsigned_bigint(1u64);
            tuple.push_string(&database_name);
            tuple.push_string(&table_name);
            tuple.push_string(&column.name.to_string());
            tuple.push_string(&type_column);
            tuple.push_boolean(notnull_column);
            tuple.push_boolean(unique_column);
            tuple.push_boolean(primary_key_column);
            tuples.push(tuple);

            let table = Table::new(
                Config::system_database(),
                Config::system_database_table_columns()
            );

            self.insert_tuples(&table, &mut tuples);
        }
        Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE TABLE")))
    }

    pub fn create_sequence(
        &mut self, 
        database_name: &String, 
        table_name: &String,
        column_name: &String,
        sequence_name: &String,
        _data_type: Option<DataType>,
        _if_not_exists: bool,
        _sequence_options: Vec<SequenceOptions>
    ) -> Result<ResultSet, ExecutionError>{

        let mut tuples: Vec<Tuple> = Vec::new();
        let mut tuple: Tuple = Tuple::new();
        tuple.push_unsigned_bigint(1);
        tuple.push_string(&database_name);
        tuple.push_string(&table_name);
        tuple.push_string(&column_name);
        tuple.push_string(&sequence_name);
        tuple.push_unsigned_bigint(1u64);
        tuples.push(tuple);

        let table = Table::new(
            Config::system_database(),
            Config::system_database_table_sequences()
        );

        self.insert_tuples(&table, &mut tuples);

        /*
        if self.context.check_table_exists(&database_name, &table_name) && if_not_exists {
            return Ok(
                ResultSet::new_command(
                    ResultSetType::Change, String::from("CREATE TABLE")
                )
            );
        }
        if self.context.check_table_exists(&database_name, &table_name) {
            return Err(ExecutionError::DatabaseExists(database_name.to_string()));
        }
        OsInterface::create_file(
            &self.pager.format_table_name(database_name, table_name)
        );
        self.context.add_table(database_name.to_string(), table_name.to_string());


        for column in columns.iter() {
            self.context.add_column(
                database_name.to_string(),
                table_name.to_string(),
                column.name.to_string(),
                ColumnType::Varchar
            );

            let mut type_column: String = String::from("");
            let mut notnull_column: bool = false;
            let mut unique_column: bool = false;
            let mut primary_key_column: bool = false;

            match column.data_type {
                DataType::BigInt(None) => { type_column = String::from("BIGINT") },
                DataType::Integer(None) => { type_column = String::from("INTEGER") },
                DataType::Varchar(None) => { type_column = String::from("VARCHAR") }
                _ => {}
            }

            for option in &column.options {
                match option.option {
                    ColumnOption::NotNull => { notnull_column = true }
                    ColumnOption::Unique { is_primary, ..} => {
                        notnull_column = true;
                        unique_column = true;
                        primary_key_column = is_primary;
                    }
                    _ => {}
                }
            }

            let mut tuples: Vec<Tuple> = Vec::new();
            let mut tuple: Tuple = Tuple::new();
            tuple.push_string(&database_name);
            tuple.push_string(&table_name);
            tuple.push_string(&column.name.to_string());
            tuple.push_string(&type_column);
            tuple.push_boolean(notnull_column);
            tuple.push_boolean(unique_column);
            tuple.push_boolean(primary_key_column);
            tuples.push(tuple);

            self.insert_tuples(
                &Config::system_database(),
                &Config::system_database_table_columns(),
                &mut tuples
            );
        }
    */
        Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE SEQUENCE")))
    }

    pub fn drop_table(
        &mut self, 
        database_name: &String, 
        table_name: &String,
        if_exists: bool
    ) -> Result<ResultSet, ExecutionError>{
        let table = Table::new(database_name.clone(), table_name.clone());
        if self.context.check_table_exists(&table) == false && if_exists {
            return Ok(
                ResultSet::new_command(
                    ResultSetType::Change, 
                    String::from("DROP TABLE")
                )
            );
        }
        if self.context.check_table_exists(&table) == false {
            return Err(ExecutionError::TableNotExists(database_name.to_string()));
        }
        OsInterface::destroy_file(
            &self.pager.format_table_name(database_name, table_name)
        );
        self.context.remove_table(database_name.to_string(), table_name.to_string());

        Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP TABLE")))
    }

    pub fn remove_database(&mut self, name: &String) -> Result<ResultSet, ExecutionError>{
        let table = Table::new(
            Config::system_database(),
            Config::system_database_table_databases()
        );
        let tuples = self.read_tuples(&table);

        let mut new_tuples: Vec<Tuple> = tuples
            .into_iter()
            .filter(|tuple| tuple.get_string(1).unwrap() != *name)
            .collect();

        let table = Table::new(
            Config::system_database(),
            Config::system_database_table_databases()
        );

        self.update_tuples(&table, &mut new_tuples);

        Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP DATABASE")))
    }

    pub fn get_columns(&mut self, table: &Table) -> Vec<Column> {
        let mut columns: Vec<Column> = Vec::new();

        let table_columns = Table::new(
            Config::system_database(),
            Config::system_database_table_columns()
        );

        let tuples: Vec<Tuple> = self.read_tuples(&table_columns)
            .into_iter()
            .filter(|tuple| {
                return tuple.get_string(1).unwrap() == *table.database_name &&
                    tuple.get_string(2).unwrap() == *table.name 
            })
            .collect();

        for elem in tuples.into_iter() {
            columns.push(
                Column::new_with_alias(
                    table.database_name.clone(),
                    table.database_alias.clone(),
                    table.name.clone(),
                    table.alias.clone(),
                    elem.get_string(3).unwrap(),
                    elem.get_string(3).unwrap(),
                    ColumnType::Varchar
                )
            );
        }

        return columns;
    }

    pub fn insert_tuples(&mut self, table: &Table, tuples: &mut Vec<Tuple>) {
        self.pager.insert_tuples(&table.database_name, &table.name, tuples);
        self.pager.flush_page(&table.database_name, &table.name);
    }

    pub fn update_tuples(&mut self, table: &Table, tuples: &mut Vec<Tuple>) {
        self.pager.update_tuples(&table.database_name, &table.name, tuples);
        self.pager.flush_page(&table.database_name, &table.name);
    }

    pub fn read_tuples(&mut self, table: &Table) -> Vec<Tuple> {
        Logger::debug(format!("Reading ({}, {})", table.database_name, table.name).leak());
        return self.pager.read_tuples(&table.database_name, &table.name)
    }

    pub fn product_cartesian(&mut self, tables: Vec<Table>) -> ResultSet {
        let columns = Vec::<Column>::new();
        let tuples: Vec<Tuple> = Vec::new();
        let mut result_set = ResultSet::new_select(columns, tuples);
        
        for (_dx, table) in tables.iter().enumerate() {
            let columns1 = self.get_columns(&table);
            let tuples1: Vec<Tuple> = self.read_tuples(&table);
            let result_set1 = ResultSet::new_select(columns1, tuples1);

            result_set = result_set.cartesian_product(&result_set1);
        }

        return result_set;
    }

}
