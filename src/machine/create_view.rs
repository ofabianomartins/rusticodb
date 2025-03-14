use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::insert_tuples;

use crate::storage::os_interface::create_file;
use crate::storage::Tuple;
use crate::storage::format_table_name;
use crate::storage::tuple_push_varchar;
use crate::storage::tuple_push_unsigned_bigint;
use crate::storage::tuple_new;

use crate::utils::ExecutionError;

use crate::config::SysDb;

pub fn create_view(
    machine: &mut Machine, 
    table: &Table, 
    query: &String
) -> Result<ResultSet, ExecutionError>{
    create_file(&format_table_name(&table.database_name, &table.name));

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = tuple_new();
    tuple_push_unsigned_bigint(&mut tuple, 1u64);
    tuple_push_varchar(&mut tuple, &table.database_name);
    tuple_push_varchar(&mut tuple, &table.name);
    tuple_push_varchar(&mut tuple, &String::from("view"));
    tuple_push_varchar(&mut tuple, query);
    tuples.push(tuple);

    insert_tuples(machine, &SysDb::table_tables(), &mut tuples);

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE VIEW")))
}
