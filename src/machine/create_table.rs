
use crate::machine::Table;
use crate::machine::Column;
use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::insert_row;
use crate::machine::get_tables_table_definition_without_id;
use crate::machine::get_columns_table_definition_without_id;
use crate::machine::create_sequence;

use crate::storage::create_file;
use crate::storage::Tuple;
use crate::storage::get_tuple_column_without_id;
use crate::storage::get_tuple_table;
use crate::storage::format_table_name;

use crate::utils::ExecutionError;

use crate::config::SysDb;

use crate::utils::Logger;

pub fn create_table(
    machine: &mut Machine, 
    table: &Table, 
    columns: Vec<Column>
) -> Result<ResultSet, ExecutionError>{
    create_file(&format_table_name(&table.database_name, &table.name));

    Logger::info(format!("CREATE TABLE {}", table.name).leak());
    let _ = insert_row(
        machine,
        &SysDb::table_tables(),
        &get_tables_table_definition_without_id(),
        &mut vec![get_tuple_table(&table.database_name, &table.name)]
    );

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

    let _ = insert_row(
        machine,
        &SysDb::table_columns(),
        &get_columns_table_definition_without_id(),
        &mut column_tuples
    );

    for column in columns.iter() {
        if column.primary_key {
            let _ = create_sequence(
                machine,
                &table.database_name,
                &table.name,
                &column.name.to_string(),
                &format!(
                    "{}_{}_{}_primary_key",
                    table.database_name,
                    table.name,
                    column.name.to_string()
                ),
                None,
                Vec::new()
            );
        }
    }
    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE TABLE")))
}
