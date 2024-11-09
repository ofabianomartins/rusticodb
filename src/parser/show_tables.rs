use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::machine::result_set::ExecutionError;

pub fn show_tables(machine: &mut Machine) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.context.actual_database.clone() {
        return machine.list_tables(db_name);
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

