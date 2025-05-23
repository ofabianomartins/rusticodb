// OBJECTS AND MESSAGES
pub mod database;
pub mod table;
pub mod column;

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
pub mod create_columns;
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
pub mod drop_sequence;

// INDEX FUNCTIONS
pub mod index;
pub mod create_index;
pub mod check_index_exists;
pub mod get_indexes;
pub mod drop_index;

// VIEW FUNCTIONS
pub mod create_view;
pub mod check_view_exists;

// TUPLE FUNCTIONS
pub mod read_tuples;
pub mod insert_row;
pub mod insert_tuples;
pub mod update_tuples;
pub mod drop_tuples;
pub mod attribution;
pub mod update_row;

// SELECT FUNCTIONS
pub mod product_cartesian;

pub use self::attribution::Attribution;

pub use self::database::{ Database, get_databases_table_definition, get_databases_table_definition_without_id };
pub use self::table::{ Table, get_tables_table_definition, get_tables_table_definition_without_id };
pub use self::column::{
    Column,
    ColumnType,
    get_columns_table_definition,
    get_columns_table_definition_without_id,
    get_rowid_column_for_table,
    map_column_type
};
pub use self::sequence::{ 
    Sequence,
    get_sequences_table_definition,
    get_sequences_table_definition_without_id,
    get_sequences_next_id_column_definition
};
pub use self::index::{ Index, get_indexes_table_definition, get_indexes_table_definition_without_id };

pub use create_file::create_file;
pub use path_exists::path_exists;
pub use database_exists::database_exists;

pub use create_database::create_database;
pub use drop_database::drop_database;
pub use drop_database_ref::drop_database_ref;
pub use check_database_exists::check_database_exists;

pub use get_tables::get_tables;
pub use create_table::create_table;
pub use create_columns::create_columns;
pub use drop_table::drop_table;
pub use drop_table_ref::drop_table_ref;
pub use check_table_exists::check_table_exists;

pub use drop_columns::drop_columns;
pub use get_columns::get_columns;

pub use create_sequence::create_sequence;
pub use get_sequence_next_id::get_sequence_next_id;
pub use get_sequences::get_sequences;
pub use check_sequence_exists::check_sequence_exists;
pub use drop_sequence::drop_sequence;

pub use create_index::create_index;
pub use get_indexes::get_indexes;
pub use check_index_exists::check_index_exists;
pub use drop_index::drop_index;

pub use create_view::create_view;
pub use check_view_exists::check_view_exists;

pub use product_cartesian::product_cartesian;

pub use insert_row::insert_row;
pub use insert_row::adjust_rows;
pub use insert_tuples::insert_tuples;
pub use update_tuples::update_tuples;
pub use read_tuples::read_tuples;
pub use drop_tuples::drop_tuples;
pub use update_row::update_row;

use crate::storage::Pager;

#[derive(Debug)]
pub struct Machine { 
    pub pager: Pager,
    pub actual_database: Option<String>
}

impl Machine {
    pub fn new(pager: Pager) -> Self {
        Self { pager, actual_database: None }
    }

    pub fn get_actual_database_name(&mut self) -> String {
        match &self.actual_database {
            Some(db) => db.clone(),
            None => String::from("<no-database>")
        }
    }

    pub fn set_actual_database(&mut self, name: String) {
        self.actual_database = Some(name);
    }
}
