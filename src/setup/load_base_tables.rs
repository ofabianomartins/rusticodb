use crate::machine::Machine;
use crate::machine::insert_row;
use crate::machine::insert_full_row;
use crate::machine::create_file;
use crate::machine::get_databases_table_definition;
use crate::machine::get_databases_table_definition_without_id;
use crate::machine::get_tables_table_definition;
use crate::machine::get_tables_table_definition_without_id;
use crate::machine::get_indexes_table_definition_without_id;

use crate::storage::Tuple;
use crate::storage::get_tuple_database;
use crate::storage::get_tuple_table;
use crate::storage::get_tuple_index;

use crate::config::SysDb;

use crate::utils::Logger;


pub fn setup_base_tables(machine: &mut Machine) {
    Logger::info("setup databases table");

    let mut tuples: Vec<Tuple> = Vec::new();
    tuples.push(get_tuple_database(&SysDb::dbname()));

    create_file(machine, &SysDb::table_databases());
    let _ = insert_full_row(
        machine,
        &SysDb::table_databases(),
        &get_databases_table_definition(),
        &get_databases_table_definition_without_id(),
        &mut tuples
    );

    Logger::info("setup tables table");

    let mut tuples: Vec<Tuple> = Vec::new();
    tuples.push(get_tuple_table(&SysDb::dbname(), &SysDb::tblname_databases()));
    tuples.push(get_tuple_table(&SysDb::dbname(), &SysDb::tblname_tables()));
    tuples.push(get_tuple_table(&SysDb::dbname(), &SysDb::tblname_columns()));
    tuples.push(get_tuple_table(&SysDb::dbname(), &SysDb::tblname_sequences()));
    tuples.push(get_tuple_table(&SysDb::dbname(), &SysDb::tblname_indexes()));

    create_file(machine, &SysDb::table_tables());
    let _ = insert_full_row(
        machine, 
        &SysDb::table_tables(),
        &get_tables_table_definition(),
        &get_tables_table_definition_without_id(),
        &mut tuples
    );

    Logger::info("setup indexes table");

    let mut tuples: Vec<Tuple> = Vec::new();
    tuples.push(get_tuple_index(&SysDb::dbname(), &SysDb::tblname_databases(), &String::from("id"), &String::from("rusticodb_databases_id"), &String::from("btree")));
    tuples.push(get_tuple_index(&SysDb::dbname(), &SysDb::tblname_tables()   , &String::from("id"), &String::from("rusticodb_tables_id"), &String::from("btree")));
    tuples.push(get_tuple_index(&SysDb::dbname(), &SysDb::tblname_columns()  , &String::from("id"), &String::from("rusticodb_columns_id"), &String::from("btree")));
    tuples.push(get_tuple_index(&SysDb::dbname(), &SysDb::tblname_sequences(), &String::from("id"), &String::from("rusticodb_sequences_id"), &String::from("btree")));
    tuples.push(get_tuple_index(&SysDb::dbname(), &SysDb::tblname_indexes()  , &String::from("id"), &String::from("rusticodb_indexes_id"), &String::from("btree")));

    create_file(machine, &SysDb::table_indexes());
    let _ = insert_row(
        machine,
        &SysDb::table_indexes(), 
        &get_indexes_table_definition_without_id(),
        &mut tuples
    );
}
