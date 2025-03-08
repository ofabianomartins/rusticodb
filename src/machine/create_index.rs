use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::insert_row;
use crate::machine::get_indexes_table_definition_without_id;

use crate::storage::Tuple;
use crate::storage::get_tuple_index;

use crate::config::SysDb;

use crate::utils::ExecutionError;

pub fn create_index(
    machine: &mut Machine, 
    database_name: &String, 
    table_name: &String,
    column_name: &String,
    index_name: &String,
    index_type: &String
) -> Result<ResultSet, ExecutionError>{

    let mut tuples: Vec<Tuple> = Vec::new();
    tuples.push(get_tuple_index(&database_name, &table_name, &column_name, &index_name, &index_type));

    let _ = insert_row(machine, &SysDb::table_indexes(), &get_indexes_table_definition_without_id(), &mut tuples);

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE INDEX")))
}
