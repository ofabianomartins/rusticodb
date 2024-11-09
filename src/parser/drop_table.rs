use sqlparser::ast::ObjectName;

use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::machine::result_set::ExecutionError;

pub fn drop_table(machine: &mut Machine, names: Vec<ObjectName>, if_exists: bool) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.context.actual_database.clone() {
        return machine.drop_table(&db_name, &names[0].to_string(), if_exists);
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

