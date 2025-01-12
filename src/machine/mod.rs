pub mod database;
pub mod table;
pub mod column;
pub mod result_set;
pub mod condition;
pub mod raw_val;

pub mod create_database;
pub mod drop_database;
pub mod drop_database_ref;

pub mod create_table;
pub mod drop_table;
pub mod drop_table_ref;

pub mod drop_columns;

pub mod create_sequence;

pub mod get_columns;
pub mod get_sequence_next_id;
pub mod get_tables;

pub mod product_cartesian;

pub mod read_tuples;

pub mod check_database_exists;
pub mod check_table_exists;

pub mod insert_row;

pub mod insert_tuples;
pub mod update_tuples;
pub mod drop_tuples;

pub use self::result_set::ResultSet;
pub use self::result_set::ResultSetType;
pub use self::condition::{ Condition, Condition1Type, Condition2Type };
pub use self::table::Table;
pub use self::column::{ Column, ColumnType };

pub use create_database::create_database;
pub use drop_database::drop_database;
pub use drop_database_ref::drop_database_ref;

pub use create_table::create_table;
pub use drop_table::drop_table;
pub use drop_table_ref::drop_table_ref;

pub use drop_columns::drop_columns;

pub use create_sequence::create_sequence;

pub use get_columns::get_columns;
pub use get_sequence_next_id::get_sequence_next_id;
pub use get_tables::get_tables;

pub use product_cartesian::product_cartesian;

pub use read_tuples::read_tuples;

pub use check_database_exists::check_database_exists;
pub use check_table_exists::check_table_exists;

pub use insert_row::insert_row;

pub use insert_tuples::insert_tuples;
pub use update_tuples::update_tuples;
pub use drop_tuples::drop_tuples;

use crate::storage::pager::Pager;
use crate::storage::os_interface::OsInterface;

use crate::utils::execution_error::ExecutionError;

#[derive(Debug)]
pub struct Machine { 
    pub pager: Pager,
    pub actual_database: Option<String>
}

impl Machine {
    pub fn new(pager: Pager) -> Self {
        Self { pager, actual_database: None }
    }

    pub fn database_exists(&mut self, database_name: &String) -> bool{
        return OsInterface::path_exists(&self.pager.format_database_name(database_name));
    }

    pub fn table_exists(&mut self, database_name: &String, table_name: &String) -> bool{
        return OsInterface::path_exists(&self.pager.format_table_name(database_name, table_name));
    }

    pub fn set_actual_database(&mut self, name: String) -> Result<ResultSet, ExecutionError> {
        if check_database_exists(self, &name) == false {
            return Err(ExecutionError::DatabaseNotExists(name));
        }
        self.actual_database = Some(name);
        Ok(ResultSet::new_command(ResultSetType::Change, String::from("USE DATABASE")))
    }

}
