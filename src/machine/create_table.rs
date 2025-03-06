
use crate::config::Config;

use crate::machine::Table;
use crate::machine::Column;
use crate::machine::ColumnType;
use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::insert_tuples;

use crate::storage::os_interface::create_file;
use crate::storage::Tuple;
use crate::storage::get_tuple_column;
use crate::storage::get_tuple_table;
use crate::storage::get_tuple_sequence;
use crate::storage::format_table_name;

use crate::utils::ExecutionError;

use crate::sys_db::SysDb;

pub fn create_table(
    machine: &mut Machine, 
    table: &Table, 
    columns: Vec<Column>
) -> Result<ResultSet, ExecutionError>{
    create_file(&format_table_name(&table.database_name, &table.name));

    insert_tuples(
        machine,
        &SysDb::table_tables(),
        &mut vec![
            get_tuple_table(1u64, &table.database_name, &table.name)
        ]
    );

    let mut column_tuples: Vec<Tuple> = vec![];
    let mut sequence_tuples: Vec<Tuple> = Vec::new();

    for column in columns.iter() {
        let type_column: String = match column.column_type {
            ColumnType::UnsignedTinyint => String::from("UNSIGNED TINYINT"),
            ColumnType::SignedTinyint => String::from("SIGNED TINYINT"),
            ColumnType::UnsignedSmallint => String::from("UNSIGNED SMALLINT"),
            ColumnType::SignedSmallint => String::from("SIGNED SMALLINT"),
            ColumnType::UnsignedInt => String::from("UNSIGNED INT"),
            ColumnType::SignedInt => String::from("SIGNED INT"),
            ColumnType::UnsignedBigint => String::from("UNSIGNED BIGINT"),
            ColumnType::SignedBigint => String::from("SIGNED BIGINT"),
            ColumnType::Varchar => String::from("VARCHAR"),
            ColumnType::Text => String::from("TEXT"),
            ColumnType::Boolean => String::from("UNSIGNED TINYINT"),
            _ => String::from("UNDEFINED")
        };

        column_tuples.push(
            get_tuple_column(
                1u64,
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

        if column.primary_key {
            sequence_tuples.push(
                get_tuple_sequence(
                    1u64,
                    &table.database_name,
                    &table.name,
                    &column.name.to_string(),
                    &format!(
                        "{}_{}_{}_primary_key",
                        table.database_name,
                        table.name,
                        column.name.to_string()
                    ),
                    1u64
                )
            );
        }
    }

    let table_columns = Table::new(Config::sysdb(), Config::sysdb_table_columns());
    let table_sequences = Table::new(Config::sysdb(), Config::sysdb_table_sequences());

    insert_tuples(machine, &table_columns, &mut column_tuples);
    insert_tuples(machine, &table_sequences, &mut sequence_tuples);
    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE TABLE")))
}
