use sqlparser::ast::ObjectName;

use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
// use crate::machine::result_set::ResultSetType;
use crate::machine::result_set::ExecutionError;

pub fn drop_database(machine: &mut Machine, names: Vec<ObjectName>, if_exists: bool) -> Result<ResultSet, ExecutionError> { 
    machine.drop_database(names[0].to_string(), if_exists)
}

