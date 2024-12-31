use sqlparser::ast::ObjectName;
use sqlparser::ast::DataType;
use sqlparser::ast::SequenceOptions;

use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::utils::execution_error::ExecutionError;

pub fn create_sequence(
    machine: &mut Machine,
    name: ObjectName,
    data_type: Option<DataType>,
    owned_by: Option<ObjectName>,
    if_not_exists: bool,
    sequence_options: Vec<SequenceOptions>
) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let mut table_name = String::from("");
        let mut column_name = String::from("");

        if let Some(ObjectName(name_list)) = owned_by {
            if name_list.len() == 2 {
                table_name = name_list[0].to_string();
                column_name = name_list[1].to_string();
            }
        }
        return machine.create_sequence(
            &db_name,
            &table_name,
            &column_name,
            &name.to_string(),
            data_type,
            if_not_exists,
            sequence_options
        )
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

