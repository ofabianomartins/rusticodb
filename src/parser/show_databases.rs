use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::utils::execution_error::ExecutionError;

pub fn show_databases(machine: &mut Machine) -> Result<ResultSet, ExecutionError> { 
    return Ok(machine.list_databases());
}

