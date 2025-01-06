use sqlparser::ast::ObjectName;

use crate::machine::Machine;
use crate::machine::table::Table;
use crate::machine::result_set::ResultSet;
use crate::utils::execution_error::ExecutionError;

pub fn drop_table(machine: &mut Machine, names: Vec<ObjectName>, if_exists: bool) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let table = Table::new(db_name, names[0].to_string());
        return machine.drop_table(&table, if_exists);
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

