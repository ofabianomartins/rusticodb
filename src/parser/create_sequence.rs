use sqlparser::ast::ObjectName;
use sqlparser::ast::DataType;
use sqlparser::ast::SequenceOptions;

use crate::machine::Machine;
use crate::machine::create_sequence as machine_create_sequence;
use crate::machine::check_sequence_exists;

use crate::storage::ResultSet;
use crate::storage::ResultSetType;

use crate::utils::ExecutionError;

pub fn create_sequence(
    machine: &mut Machine,
    name: ObjectName,
    data_type: Option<DataType>,
    owned_by: Option<ObjectName>,
    if_not_exists: bool,
    sequence_options: Vec<SequenceOptions>
) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        if check_sequence_exists(machine, &db_name, &name.to_string()) && if_not_exists {
            return Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE SEQUENCE")));
        }
        if check_sequence_exists(machine, &db_name, &name.to_string()) {
            return Err(ExecutionError::SequenceExists(db_name));
        }
        let mut table_name = String::from("");
        let mut column_name = String::from("");

        if let Some(ObjectName(name_list)) = owned_by {
            if name_list.len() == 2 {
                table_name = name_list[0].to_string();
                column_name = name_list[1].to_string();
            }
        }
        return machine_create_sequence(
            machine,
            &db_name,
            &table_name,
            &column_name,
            &name.to_string(),
            data_type,
            sequence_options
        )
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

