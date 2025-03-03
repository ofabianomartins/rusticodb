use sqlparser::ast::CreateTable;

use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::create_table as machine_create_table;
use crate::machine::check_table_exists;

use crate::utils::ExecutionError;

pub fn create_table(machine: &mut Machine, create_table: CreateTable) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let if_not_exists = create_table.if_not_exists;
        let table = Table::new(db_name, create_table.name.to_string());

        if check_table_exists(machine, &table) && if_not_exists {
            return Ok(
                ResultSet::new_command(
                    ResultSetType::Change, String::from("CREATE TABLE")
                )
            );
        }
        if check_table_exists(machine, &table) {
            return Err(ExecutionError::DatabaseExists(table.database_name.to_string()));
        }

        return machine_create_table(
            machine,
            &table,
            create_table.columns
        );
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

