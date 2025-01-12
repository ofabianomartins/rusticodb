use sqlparser::ast::CreateTable;

use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::ResultSet;
use crate::machine::create_table as machine_create_table;
use crate::utils::execution_error::ExecutionError;

pub fn create_table(machine: &mut Machine, create_table: CreateTable) -> Result<ResultSet, ExecutionError> { 

    if let Some(db_name) = machine.actual_database.clone() {
        let table = Table::new(db_name, create_table.name.to_string());
        return machine_create_table(
            machine,
            &table,
            create_table.if_not_exists,
            create_table.columns
        );
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

