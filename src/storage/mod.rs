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
pub use self::tuple::tuple_new;
pub use self::tuple::tuple_append_cell;
pub use self::tuple::tuple_get_cell;
pub use self::tuple::tuple_push_null;
pub use self::tuple::tuple_push_varchar;
pub use self::tuple::tuple_push_text;
pub use self::tuple::tuple_push_boolean;
pub use self::tuple::tuple_push_unsigned_tinyint;
pub use self::tuple::tuple_push_unsigned_smallint;
pub use self::tuple::tuple_push_unsigned_int;
pub use self::tuple::tuple_push_unsigned_bigint;
pub use self::tuple::tuple_push_signed_tinyint;
pub use self::tuple::tuple_push_signed_smallint;
pub use self::tuple::tuple_push_signed_int;
pub use self::tuple::tuple_push_signed_bigint;
pub use self::tuple::tuple_get_vec_u8;
pub use self::tuple::tuple_get_varchar;
pub use self::tuple::tuple_get_text;
pub use self::tuple::tuple_get_boolean;
pub use self::tuple::tuple_get_unsigned_tinyint;
pub use self::tuple::tuple_get_unsigned_smallint;
pub use self::tuple::tuple_get_unsigned_int;
pub use self::tuple::tuple_get_unsigned_bigint;
pub use self::tuple::tuple_get_signed_tinyint;
pub use self::tuple::tuple_get_signed_smallint;
pub use self::tuple::tuple_get_signed_int;
pub use self::tuple::tuple_get_signed_bigint;
pub use self::tuple::tuple_set_cell_count;
pub use self::tuple::tuple_cell_count;
pub use self::tuple::tuple_set_data_size;
pub use self::tuple::tuple_data_size;
pub use self::tuple::tuple_to_raw_data;
pub use self::tuple::get_tuple_database;
pub use self::tuple::get_tuple_column;
pub use self::tuple::get_tuple_column_without_id;
pub use self::tuple::get_tuple_table;
pub use self::tuple::get_tuple_sequence;
pub use self::tuple::get_tuple_sequence_without_id;
pub use self::tuple::get_tuple_index;

pub use self::cell::{ Cell, CellType };

pub use self::insert_tuples::insert_tuples;
pub use self::update_tuples::update_tuples;
pub use self::read_tuples::read_tuples;
pub use self::read_data::read_data;
pub use self::write_data::write_data;
pub use self::flush_page::flush_page;

pub use self::page::Page;
pub use self::page::page_new;
pub use self::page::page_set_tuple_count;
pub use self::page::page_tuple_count;
pub use self::page::page_set_next_tuple_position;
pub use self::page::page_next_tuple_position;
pub use self::page::page_insert_tuples;
pub use self::page::page_update_tuples;
pub use self::page::page_read_tuples;

pub use self::os_interface::{ BLOCK_SIZE };

pub type Pager = HashMap<String, Page>;
