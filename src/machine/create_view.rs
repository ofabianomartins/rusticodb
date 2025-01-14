use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::insert_tuples;

use crate::storage::OsInterface;
use crate::storage::Tuple;

use crate::utils::ExecutionError;

use crate::sys_db::SysDb;

pub fn create_view(
    machine: &mut Machine, 
    table: &Table, 
    query: &String
) -> Result<ResultSet, ExecutionError>{
    OsInterface::create_file(
        &machine.pager.format_table_name(&table.database_name, &table.name)
    );

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_string(&table.database_name);
    tuple.push_string(&table.name);
    tuple.push_string(&String::from("view"));
    tuple.push_string(query);
    tuples.push(tuple);

    insert_tuples(machine, &SysDb::table_tables(), &mut tuples);

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE VIEW")))
}
