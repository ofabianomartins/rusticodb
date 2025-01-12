use sqlparser::ast::ObjectName;

use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::ResultSet;
use crate::machine::drop_table as machine_drop_table;

use crate::utils::ExecutionError;

pub fn drop_table(machine: &mut Machine, names: Vec<ObjectName>, if_exists: bool) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let table = Table::new(db_name, names[0].to_string());
        return machine_drop_table(machine, &table, if_exists);
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

