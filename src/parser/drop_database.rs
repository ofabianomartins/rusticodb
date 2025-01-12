use sqlparser::ast::ObjectName;

use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::drop_database as machine_drop_database;
use crate::utils::execution_error::ExecutionError;

pub fn drop_database(machine: &mut Machine, names: Vec<ObjectName>, if_exists: bool) -> Result<ResultSet, ExecutionError> { 
    machine_drop_database(machine, names[0].to_string(), if_exists)
}

