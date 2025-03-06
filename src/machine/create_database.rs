use crate::config::Config;

use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::check_database_exists;
use crate::machine::insert_tuples;

use crate::storage::os_interface::create_folder;
use crate::storage::Tuple;
use crate::storage::format_database_name;

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
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_string(&database_name);
    tuples.push(tuple);

    let table = Table::new(Config::sysdb(), Config::sysdb_table_databases());

    insert_tuples(machine, &table, &mut tuples);

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE DATABASE")))
}
