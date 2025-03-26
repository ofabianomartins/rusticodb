use crate::machine::Machine;
use crate::machine::get_tables;
use crate::machine::drop_columns;
use crate::machine::drop_table_ref;
use crate::machine::drop_database_ref;
use crate::machine::check_database_exists;

use crate::storage::destroy_folder;
use crate::storage::format_database_name;
use crate::storage::ResultSet;
use crate::storage::ResultSetType;

use crate::utils::ExecutionError;

pub fn drop_database(machine: &mut Machine, database_name: String, if_exists: bool) -> Result<ResultSet, ExecutionError>{
    if check_database_exists(machine, &database_name) == false && if_exists {
        return Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP DATABASE")));
    }
    if check_database_exists(machine, &database_name) == false {
        return Err(ExecutionError::DatabaseNotExists(database_name));
    }

    for table in get_tables(machine, &database_name) {
        drop_columns(machine, &table);
        drop_table_ref(machine, &table);
    }
    drop_database_ref(machine, &database_name);

    destroy_folder(&format_database_name(&database_name));

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP DATABASE")))
}
