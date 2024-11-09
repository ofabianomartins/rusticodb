use crate::machine::column::Column;
use crate::machine::column::ColumnType;
use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::machine::result_set::ExecutionError;
use crate::config::Config;

pub fn show_databases(machine: &mut Machine) -> Result<ResultSet, ExecutionError> { 
    let db_name = Config::system_database();
    let table_databases = Config::system_database_table_databases();
    let mut columns: Vec<Column> = Vec::new();
    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));
    let tuples = machine.read_tuples(&db_name, &table_databases);
    return Ok(ResultSet::new_select(columns, tuples))
}

