use sqlparser::ast::Query;

use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::create_view as machine_create_view;
use crate::machine::check_view_exists;

use crate::utils::ExecutionError;

pub fn create_view(
    machine: &mut Machine, 
    name: &String,
    query: Box<Query>,
    or_replace: bool,
    if_not_exists: bool
) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {

        let table = Table::new(db_name, name.to_string());

        if check_view_exists(machine, &table) && or_replace {
            return Ok(
                ResultSet::new_command(
                    ResultSetType::Change, String::from("CREATE VIEW")
                )
            );
        }

        if check_view_exists(machine, &table) && if_not_exists {
            return Ok(
                ResultSet::new_command(
                    ResultSetType::Change, String::from("CREATE VIEW")
                )
            );
        }
        if check_view_exists(machine, &table) {
            return Err(ExecutionError::ViewExists(name.clone()));
        }

        return machine_create_view(
            machine,
            &table,
            &format!("{}", *query.body)
        );
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

