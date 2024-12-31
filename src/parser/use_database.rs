use sqlparser::ast::Use;

use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::utils::execution_error::ExecutionError;

pub fn use_database(machine: &mut Machine, statement: Use) -> Result<ResultSet, ExecutionError> { 
    match statement {
        Use::Object(db_name) => machine.set_actual_database(db_name.to_string()),
        value => { 
            println!("USE {:?}", value);
            Err(ExecutionError::NotImplementedYet)
        }
    }
}

