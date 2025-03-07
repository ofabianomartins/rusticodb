use crate::config::Config;

use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::check_database_exists;
use crate::machine::insert_row;
use crate::machine::get_databases_table_definition_without_id;

use crate::storage::os_interface::create_folder;
use crate::storage::Tuple;
use crate::storage::format_database_name;
use crate::storage::get_tuple_database;

use crate::utils::ExecutionError;

pub fn create_database(machine: &mut Machine, database_name: String, if_not_exists: bool) -> Result<ResultSet, ExecutionError>{
    if check_database_exists(machine, &database_name) && if_not_exists {
        return Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE DATABASE")));
    }
    if check_database_exists(machine, &database_name) {
        return Err(ExecutionError::DatabaseExists(database_name));
    }
    create_folder(&format_database_name(&database_name));

    let mut tuples: Vec<Tuple> = Vec::new();
    tuples.push(get_tuple_database(&database_name));

    let table = Table::new(Config::sysdb(), Config::sysdb_table_databases());

    let _ = insert_row(
        machine,
        &table,
        &get_databases_table_definition_without_id(),
        &mut tuples
    );

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE DATABASE")))
}
