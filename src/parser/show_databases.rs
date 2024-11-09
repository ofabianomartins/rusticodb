use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::machine::result_set::ExecutionError;

pub fn show_databases(machine: &mut Machine) -> Result<ResultSet, ExecutionError> { 
    return Ok(machine.list_databases());
}

