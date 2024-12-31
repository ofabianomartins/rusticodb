use sqlparser::ast::CreateTable;

use crate::machine::machine::Machine;
use crate::machine::table::Table;
use crate::machine::result_set::ResultSet;
use crate::utils::execution_error::ExecutionError;

pub fn create_table(machine: &mut Machine, create_table: CreateTable) -> Result<ResultSet, ExecutionError> { 

    if let Some(db_name) = machine.actual_database.clone() {
        let table = Table::new(db_name, create_table.name.to_string());
        return machine.create_table(
            &table,
            create_table.if_not_exists,
            create_table.columns
        );
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

