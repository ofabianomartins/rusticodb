use crate::machine::Table;
use crate::machine::Column;
use crate::machine::Machine;
use crate::machine::insert_row;
use crate::machine::get_columns_table_definition_without_id;
use crate::machine::get_columns;

use crate::storage::Tuple;
use crate::storage::get_tuple_column_without_id;
use crate::storage::ResultSet;
use crate::storage::ResultSetType;

use crate::utils::ExecutionError;

use crate::config::SysDb;

pub fn create_columns(
    machine: &mut Machine, 
    table: &Table, 
    columns: &Vec<Column>
) -> Result<ResultSet, ExecutionError>{
    let mut column_tuples: Vec<Tuple> = vec![];

    for column in columns.iter() {
        let type_column: String = column.clone().get_type_column();
        column_tuples.push(
            get_tuple_column_without_id(
                &table.database_name,
                &table.name,
                &column.name.to_string(),
                &type_column,
                column.not_null,
                column.unique,
                column.primary_key,
                &column.default
            )
        );
    }

    let table_columns = &get_columns(machine, &SysDb::table_columns());
    if let Err(err) = insert_row(
        machine,
        &SysDb::table_columns(),
        table_columns,
        &get_columns_table_definition_without_id(),
        &mut column_tuples,
        false
    ) {
        return Err(err);
    }

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE COLUMNS")))
}
