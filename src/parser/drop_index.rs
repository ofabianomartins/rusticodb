use sqlparser::ast::ObjectName;

use crate::machine::Machine;
use crate::machine::drop_index as machine_drop_index;
use crate::machine::check_index_exists;

use crate::storage::ResultSet;
use crate::storage::ResultSetType;

use crate::utils::ExecutionError;

pub fn drop_index(machine: &mut Machine, names: Vec<ObjectName>, if_exists: bool) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let name = names[0].to_string();

        if check_index_exists(machine, &db_name, &name) == false && if_exists {
            return Ok(
                ResultSet::new_command(
                    ResultSetType::Change, 
                    String::from("DROP INDEX")
                )
            );
        }
        if check_index_exists(machine, &db_name, &name) == false {
            return Err(ExecutionError::IndexNotExists(name));
        }
        return machine_drop_index(machine, &name);
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

