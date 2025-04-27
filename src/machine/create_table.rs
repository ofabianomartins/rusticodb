
use crate::machine::Table;
use crate::machine::Column;
use crate::machine::Machine;
use crate::machine::insert_row;
use crate::machine::get_tables_table_definition_without_id;
use crate::machine::create_sequence;
use crate::machine::create_columns;
use crate::machine::get_columns;

use crate::storage::create_file;
use crate::storage::get_tuple_table;
use crate::storage::format_table_name;
use crate::storage::header_serialize;
use crate::storage::header_new;
use crate::storage::write_data;
use crate::storage::ResultSet;
use crate::storage::ResultSetType;

use crate::utils::ExecutionError;

use crate::config::SysDb;

use crate::utils::Logger;

pub fn create_table(
    machine: &mut Machine, 
    table: &Table, 
    columns: Vec<Column>
) -> Result<ResultSet, ExecutionError>{

    Logger::info(format!("CREATE TABLE {}", table.name).leak());
    let table_columns = &get_columns(machine, &SysDb::table_tables());
    let _ = insert_row(
        machine,
        &SysDb::table_tables(),
        table_columns,
        &get_tables_table_definition_without_id(),
        &mut vec![get_tuple_table(&table.database_name, &table.name)],
        false
    );

    if let Err(err) = create_columns(machine, table, &columns) {
        return Err(err);
    }

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

    let table_key = format_table_name(&table.database_name, &table.name);
    create_file(&table_key);
    write_data(&table_key, 0, &header_serialize(&header_new()));

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE TABLE")))
}
