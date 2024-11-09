use sqlparser::ast::ObjectName;

use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::machine::result_set::ResultSetType;
use crate::machine::result_set::ExecutionError;

pub fn drop_table(_machine: &mut Machine, _names: Vec<ObjectName>, _if_exists: bool) -> Result<ResultSet, ExecutionError> { 
    Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP TABLE")))
}

