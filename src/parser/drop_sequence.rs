use sqlparser::ast::ObjectName;

use crate::machine::Machine;
use crate::machine::drop_sequence as machine_drop_sequence;
use crate::machine::check_sequence_exists;

use crate::storage::ResultSet;
use crate::storage::ResultSetType;

use crate::utils::ExecutionError;

pub fn drop_sequence(machine: &mut Machine, names: Vec<ObjectName>, if_exists: bool) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let name = names[0].to_string();

        if check_sequence_exists(machine, &db_name, &name) == false && if_exists {
            return Ok(
                ResultSet::new_command(
                    ResultSetType::Change, 
                    String::from("DROP SEQUENCE")
                )
            );
        }
        if check_sequence_exists(machine, &db_name, &name) == false {
            return Err(ExecutionError::SequenceNotExists(name));
        }
        return machine_drop_sequence(machine, &name);
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

