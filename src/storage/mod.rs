use std::collections::HashMap;

pub mod page;
pub mod cell;
pub mod tuple;

pub mod os_interface;

pub mod format_database_name;
pub mod format_table_name;

pub mod insert_tuples;
pub mod update_tuples;
pub mod read_tuples;
pub mod read_data;
pub mod write_data;
pub mod flush_page;

pub use self::format_database_name::format_database_name;
pub use self::format_table_name::format_table_name;

pub use self::tuple::Tuple;
pub use self::tuple::get_tuple_database;
pub use self::tuple::get_tuple_column;
pub use self::tuple::get_tuple_table;
pub use self::tuple::get_tuple_sequence;
pub use self::cell::{ Cell, CellType };

pub use self::insert_tuples::insert_tuples;
pub use self::update_tuples::update_tuples;
pub use self::read_tuples::read_tuples;
pub use self::read_data::read_data;
pub use self::write_data::write_data;
pub use self::flush_page::flush_page;

pub use self::page::Page;

pub use self::os_interface::{ OsInterface, BLOCK_SIZE };

pub type Pager = HashMap<String, Page>;
