use sqlparser::ast::DataType;
use sqlparser::ast::SequenceOptions;

use crate::config::Config;

use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::insert_tuples;

use crate::storage::Tuple;

use crate::utils::execution_error::ExecutionError;

pub fn create_sequence(
    machine: &mut Machine, 
    database_name: &String, 
    table_name: &String,
    column_name: &String,
    sequence_name: &String,
    _data_type: Option<DataType>,
    _if_not_exists: bool,
    _sequence_options: Vec<SequenceOptions>
) -> Result<ResultSet, ExecutionError>{

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1);
    tuple.push_string(&database_name);
    tuple.push_string(&table_name);
    tuple.push_string(&column_name);
    tuple.push_string(&sequence_name);
    tuple.push_unsigned_bigint(1u64);
    tuples.push(tuple);

    let table = Table::new(
        Config::system_database(),
        Config::system_database_table_sequences()
    );

    insert_tuples(machine, &table, &mut tuples);

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE SEQUENCE")))
}
