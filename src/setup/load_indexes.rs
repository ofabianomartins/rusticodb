
use crate::config::Config;
use crate::sys_db::SysDb;

use crate::machine::Machine;
use crate::machine::insert_row;
use crate::machine::create_file;
use crate::machine::get_indexes_table_definition_without_id;

use crate::storage::Tuple;
use crate::storage::get_tuple_index;

use crate::utils::Logger;

pub fn setup_indexes_table(machine: &mut Machine) {
    Logger::info("setup indexes table");

    let mut tuples: Vec<Tuple> = Vec::new();

    tuples.push(get_tuple_index(&Config::sysdb(), &Config::sysdb_table_databases(), &String::from("id"), &String::from("rusticodb_databases_id"), &String::from("btree")));
    tuples.push(get_tuple_index(&Config::sysdb(), &Config::sysdb_table_tables()   , &String::from("id"), &String::from("rusticodb_tables_id"), &String::from("btree")));
    tuples.push(get_tuple_index(&Config::sysdb(), &Config::sysdb_table_columns()  , &String::from("id"), &String::from("rusticodb_columns_id"), &String::from("btree")));
    tuples.push(get_tuple_index(&Config::sysdb(), &Config::sysdb_table_sequences(), &String::from("id"), &String::from("rusticodb_sequences_id"), &String::from("btree")));
    tuples.push(get_tuple_index(&Config::sysdb(), &Config::sysdb_table_indexes()  , &String::from("id"), &String::from("rusticodb_indexes_id"), &String::from("btree")));

    create_file(machine, &SysDb::table_indexes());
    let _ = insert_row(
        machine,
        &SysDb::table_indexes(), 
        &get_indexes_table_definition_without_id(),
        &mut tuples
    );
}
