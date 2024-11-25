use sqlparser::ast::CreateTable;

use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::utils::execution_error::ExecutionError;

pub fn create_table(machine: &mut Machine, create_table: CreateTable) -> Result<ResultSet, ExecutionError> { 

    if let Some(db_name) = machine.context.actual_database.clone() {
        return machine.create_table(
            &db_name,
            &create_table.name.to_string(),
            create_table.if_not_exists,
            create_table.columns
        );
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

