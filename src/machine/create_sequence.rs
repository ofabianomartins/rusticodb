use sqlparser::ast::DataType;
use sqlparser::ast::SequenceOptions;

use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::insert_row;
use crate::machine::get_sequences_table_definition_without_id;

use crate::storage::Tuple;
use crate::storage::get_tuple_sequence_without_id;

use crate::config::SysDb;

use crate::utils::ExecutionError;
use crate::utils::Logger;

pub fn create_sequence(
    machine: &mut Machine, 
    database_name: &String, 
    table_name: &String,
    column_name: &String,
    sequence_name: &String,
    _data_type: Option<DataType>,
    _sequence_options: Vec<SequenceOptions>
) -> Result<ResultSet, ExecutionError>{

    let mut tuples: Vec<Tuple> = Vec::new();
    tuples.push(
        get_tuple_sequence_without_id(
            &database_name,
            &table_name,
            &column_name,
            &sequence_name,
            1u64
        )
    );

    Logger::info(format!("CREATE SEQUENCE {}", sequence_name).leak());
    let _ = insert_row(
        machine, 
        &SysDb::table_sequences(),
        &get_sequences_table_definition_without_id(),
        &mut tuples
    );

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE SEQUENCE")))
}
