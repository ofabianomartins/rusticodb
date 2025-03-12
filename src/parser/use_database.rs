use sqlparser::ast::Use;

use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::check_database_exists;

use crate::utils::ExecutionError;

pub fn use_database(machine: &mut Machine, statement: Use) -> Result<ResultSet, ExecutionError> { 
    match statement {
        Use::Object(db_name) => {
            let name = db_name.to_string();

            if check_database_exists(machine, &name) == false {
                return Err(ExecutionError::DatabaseNotExists(name));
            }
            machine.set_actual_database(name);
            return Ok(ResultSet::new_command(ResultSetType::Change, String::from("USE DATABASE")))
        },
        _ => { 
            Err(ExecutionError::NotImplementedYet)
        }
    }
}

