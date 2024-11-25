use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::utils::execution_error::ExecutionError;

pub fn create_database(machine: &mut Machine, db_name: String, if_not_exists: bool) -> Result<ResultSet, ExecutionError> { 
    machine.create_database(db_name.to_string(), if_not_exists)
}

