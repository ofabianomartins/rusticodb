use sqlparser::ast::CreateIndex;

use crate::machine::Machine;
use crate::machine::check_index_exists;
use crate::machine::create_index as machine_create_index;
use crate::utils::ExecutionError;

use crate::storage::ResultSet;
use crate::storage::ResultSetType;

pub fn create_index(machine: &mut Machine, create_index: CreateIndex) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let if_not_exists = create_index.if_not_exists;
        let table_name = create_index.table_name.to_string();
        let mut column_name = String::from("");

        if let Some(column_obj) = create_index.columns.get(0) {
            column_name = column_obj.expr.to_string();
        }

        let name;
        if let Some(name_obj) = create_index.name {
            name = name_obj.to_string();
        } else {
            name = format!("index_{}_{}", table_name, column_name);
        }

        if check_index_exists(machine, &db_name, &name.to_string()) && if_not_exists {
            return Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE INDEX")));
        }
        if check_index_exists(machine, &db_name, &name.to_string()) {
            return Err(ExecutionError::IndexExists(db_name));
        }

        return machine_create_index(
            machine,
            &db_name,
            &table_name,
            &column_name,
            &name,
            &String::from("btree")
        )
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

