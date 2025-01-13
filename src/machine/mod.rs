// OBJECTS AND MESSAGES
pub mod database;
pub mod table;
pub mod column;
pub mod result_set;
pub mod condition;
pub mod raw_val;

// FILE FUNCTIONS
pub mod create_file;
pub mod path_exists;
pub mod database_exists;

// DATABASE FUNCTIONS
pub mod create_database;
pub mod drop_database;
pub mod drop_database_ref;
pub mod check_database_exists;

// TABLE FUNCTIONS
pub mod create_table;
pub mod drop_table;
pub mod drop_table_ref;
pub mod get_tables;
pub mod check_table_exists;

// COLUMN FUNCTIONS
pub mod drop_columns;
pub mod get_columns;

// SEQUENCE FUNCTIONS
pub mod sequence;
pub mod create_sequence;
pub mod get_sequence_next_id;
pub mod check_sequence_exists;
pub mod get_sequences;

// INDEX FUNCTIONS
pub mod index;
pub mod create_index;
pub mod check_index_exists;
pub mod get_indexes;


// TUPLE FUNCTIONS
pub mod read_tuples;
pub mod insert_row;
pub mod insert_tuples;
pub mod update_tuples;
pub mod drop_tuples;

// SELECT FUNCTIONS
pub mod product_cartesian;

pub use self::result_set::ResultSet;
pub use self::result_set::ResultSetType;
pub use self::condition::{ Condition, Condition1Type, Condition2Type };
pub use self::table::Table;
pub use self::column::{ Column, ColumnType };
pub use self::sequence::Sequence;
pub use self::index::Index;

pub use create_file::create_file;
pub use path_exists::path_exists;
pub use database_exists::database_exists;

pub use create_database::create_database;
pub use drop_database::drop_database;
pub use drop_database_ref::drop_database_ref;
pub use check_database_exists::check_database_exists;

pub use get_tables::get_tables;
pub use create_table::create_table;
pub use drop_table::drop_table;
pub use drop_table_ref::drop_table_ref;
pub use check_table_exists::check_table_exists;

pub use drop_columns::drop_columns;
pub use get_columns::get_columns;

pub use create_sequence::create_sequence;
pub use get_sequence_next_id::get_sequence_next_id;
pub use get_sequences::get_sequences;
pub use check_sequence_exists::check_sequence_exists;

pub use create_index::create_index;
pub use get_indexes::get_indexes;
pub use check_index_exists::check_index_exists;

pub use product_cartesian::product_cartesian;

pub use insert_row::insert_row;
pub use insert_tuples::insert_tuples;
pub use update_tuples::update_tuples;
pub use read_tuples::read_tuples;
pub use drop_tuples::drop_tuples;

use crate::storage::Pager;

use crate::utils::ExecutionError;

#[derive(Debug)]
pub struct Machine { 
    pub pager: Pager,
    pub actual_database: Option<String>
}

impl Machine {
    pub fn new(pager: Pager) -> Self {
        Self { pager, actual_database: None }
    }

    pub fn set_actual_database(&mut self, name: String) -> Result<ResultSet, ExecutionError> {
        if check_database_exists(self, &name) == false {
            return Err(ExecutionError::DatabaseNotExists(name));
        }
        self.actual_database = Some(name);
        Ok(ResultSet::new_command(ResultSetType::Change, String::from("USE DATABASE")))
    }

}
