use crate::machine::Machine;
use crate::machine::check_database_exists;
use crate::machine::insert_row;
use crate::machine::get_databases_table_definition_without_id;

use crate::storage::create_folder;
use crate::storage::format_database_name;
use crate::storage::get_tuple_database;
use crate::storage::ResultSet;
use crate::storage::ResultSetType;

use crate::utils::ExecutionError;

use crate::config::SysDb;

pub fn create_database(machine: &mut Machine, database_name: String, if_not_exists: bool) -> Result<ResultSet, ExecutionError>{
    if check_database_exists(machine, &database_name) && if_not_exists {
        return Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE DATABASE")));
    }
    if check_database_exists(machine, &database_name) {
        return Err(ExecutionError::DatabaseExists(database_name));
    }
    create_folder(&format_database_name(&database_name));

    let _ = insert_row(
        machine,
        &SysDb::table_databases(),
        &get_databases_table_definition_without_id(),
        &mut vec![get_tuple_database(&database_name)]
    );

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE DATABASE")))
}
