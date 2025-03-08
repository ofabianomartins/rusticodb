use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::insert_tuples;

use crate::storage::os_interface::create_file;
use crate::storage::Tuple;
use crate::storage::format_table_name;

use crate::utils::ExecutionError;

use crate::config::SysDb;

pub fn create_view(
    machine: &mut Machine, 
    table: &Table, 
    query: &String
) -> Result<ResultSet, ExecutionError>{
    create_file(&format_table_name(&table.database_name, &table.name));

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_varchar(&table.database_name);
    tuple.push_varchar(&table.name);
    tuple.push_varchar(&String::from("view"));
    tuple.push_varchar(query);
    tuples.push(tuple);

    insert_tuples(machine, &SysDb::table_tables(), &mut tuples);

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE VIEW")))
}
