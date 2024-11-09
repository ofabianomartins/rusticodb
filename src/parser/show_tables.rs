use sqlparser::ast::Ident;

use crate::machine::column::Column;
use crate::machine::column::ColumnType;
use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::machine::result_set::ExecutionError;
use crate::config::Config;

pub fn show_tables(machine: &mut Machine, db_name: Option<Ident>) -> Result<ResultSet, ExecutionError> { 
    let mut columns: Vec<Column> = Vec::new();
    columns.push(Column::new_column(String::from("database"), ColumnType::Varchar));
    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));
    let mut db_name_str: String = Config::system_database();
    let table_tables: String = Config::system_database_table_tables();
    if let Some(data) = db_name {
        db_name_str = data.to_string();
    }
    let tuples = machine.read_tuples(&db_name_str, &table_tables);
    return Ok(ResultSet::new_select(columns, tuples))
}

