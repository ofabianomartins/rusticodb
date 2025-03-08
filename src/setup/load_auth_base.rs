use crate::config::Config;

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

use crate::sys_db::SysDb;
use crate::utils::Logger;


pub fn setup_auth_base(machine: &mut Machine) {
    Logger::info("setup auth databases and tables");

    let mut tuples: Vec<Tuple> = Vec::new();
    tuples.push(get_tuple_database(&Config::sysdb()));

    create_file(machine, &SysDb::table_databases());
    let _ = insert_full_row(
        machine,
        &SysDb::table_databases(),
        &get_databases_table_definition(),
        &get_databases_table_definition_without_id(),
        &mut tuples
    );

}
