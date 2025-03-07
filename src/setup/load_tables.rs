use crate::config::Config;

use crate::machine::Machine;
use crate::machine::insert_full_row;
use crate::machine::create_file;
use crate::machine::get_tables_table_definition;
use crate::machine::get_tables_table_definition_without_id;

use crate::storage::Tuple;
use crate::storage::get_tuple_table;

use crate::utils::Logger;

use crate::sys_db::SysDb;

pub fn setup_tables_table(machine: &mut Machine) {
    Logger::info("setup tables table");

    let mut tuples: Vec<Tuple> = Vec::new();
    tuples.push(get_tuple_table(&Config::sysdb(), &Config::sysdb_table_databases()));
    tuples.push(get_tuple_table(&Config::sysdb(), &Config::sysdb_table_tables()));
    tuples.push(get_tuple_table(&Config::sysdb(), &Config::sysdb_table_columns()));
    tuples.push(get_tuple_table(&Config::sysdb(), &Config::sysdb_table_sequences()));
    tuples.push(get_tuple_table(&Config::sysdb(), &Config::sysdb_table_indexes()));

    create_file(machine, &SysDb::table_tables());
    let _ = insert_full_row(
        machine, 
        &SysDb::table_tables(),
        &get_tables_table_definition(),
        &get_tables_table_definition_without_id(),
        &mut tuples
    );
}
