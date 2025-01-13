use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::insert_tuples;

use crate::storage::Tuple;

use crate::sys_db::SysDb;

use crate::utils::ExecutionError;

pub fn create_index(
    machine: &mut Machine, 
    database_name: &String, 
    table_name: &String,
    column_name: &String,
    sequence_name: &String,
    index_type: &String,
    _if_not_exists: bool
) -> Result<ResultSet, ExecutionError>{

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1);
    tuple.push_string(&database_name);
    tuple.push_string(&table_name);
    tuple.push_string(&column_name);
    tuple.push_string(&sequence_name);
    tuple.push_string(&index_type);
    tuples.push(tuple);

    insert_tuples(machine, &SysDb::table_indexes(), &mut tuples);

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE INDEX")))
}
