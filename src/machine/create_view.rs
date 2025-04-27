use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::insert_tuples;

use crate::storage::create_file;
use crate::storage::Tuple;
use crate::storage::Data;
use crate::storage::format_table_name;
use crate::storage::tuple_new;
use crate::storage::ResultSet;
use crate::storage::ResultSetType;

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
    tuple.push(Data::UnsignedBigint(1u64));
    tuple.push(Data::Varchar(table.database_name.clone()));
    tuple.push(Data::Varchar(table.name.clone()));
    tuple.push(Data::Varchar("view".to_string()));
    tuple.push(Data::Varchar(query.clone()));
    tuples.push(tuple);

    insert_tuples(machine, &SysDb::table_tables(), &mut tuples);

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE VIEW")))
}
