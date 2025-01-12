use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOption;
use sqlparser::ast::DataType;

use crate::config::Config;

use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::check_table_exists;
use crate::machine::insert_tuples;

use crate::storage::os_interface::OsInterface;
use crate::storage::Tuple;


use crate::utils::execution_error::ExecutionError;

pub fn create_table(
    machine: &mut Machine, 
    table: &Table, 
    if_not_exists: bool,
    columns: Vec<ColumnDef>
) -> Result<ResultSet, ExecutionError>{
    if check_table_exists(machine, &table) && if_not_exists {
        return Ok(
            ResultSet::new_command(
                ResultSetType::Change, String::from("CREATE TABLE")
            )
        );
    }
    if check_table_exists(machine, &table) {
        return Err(ExecutionError::DatabaseExists(table.database_name.to_string()));
    }
    OsInterface::create_file(
        &machine.pager.format_table_name(&table.database_name, &table.name)
    );

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_string(&table.database_name);
    tuple.push_string(&table.name);
    tuples.push(tuple);

    let table_tables = Table::new(
        Config::system_database(),
        Config::system_database_table_tables()
    );

    insert_tuples(machine, &table_tables, &mut tuples);

    for column in columns.iter() {
        let mut type_column: String = String::from("");
        let mut notnull_column: bool = false;
        let mut unique_column: bool = false;
        let mut primary_key_column: bool = false;

        match column.data_type {
            DataType::BigInt(None) => { type_column = String::from("BIGINT") },
            DataType::Integer(None) => { type_column = String::from("INTEGER") },
            DataType::Varchar(None) => { type_column = String::from("VARCHAR") }
            _ => {}
        }

        for option in &column.options {
            match option.option {
                ColumnOption::NotNull => { notnull_column = true }
                ColumnOption::Unique { is_primary, ..} => {
                    notnull_column = true;
                    unique_column = true;
                    primary_key_column = is_primary;
                }
                _ => {}
            }
        }

        let mut tuples: Vec<Tuple> = Vec::new();
        let mut tuple: Tuple = Tuple::new();
        tuple.push_unsigned_bigint(1u64);
        tuple.push_string(&table.database_name);
        tuple.push_string(&table.name);
        tuple.push_string(&column.name.to_string());
        tuple.push_string(&type_column);
        tuple.push_boolean(notnull_column);
        tuple.push_boolean(unique_column);
        tuple.push_boolean(primary_key_column);
        tuples.push(tuple);

        let table_columns = Table::new(
            Config::system_database(),
            Config::system_database_table_columns()
        );

        insert_tuples(machine, &table_columns, &mut tuples);

        if primary_key_column {
            let mut tuples: Vec<Tuple> = Vec::new();
            let mut tuple: Tuple = Tuple::new();
            tuple.push_unsigned_bigint(1u64);
            tuple.push_string(&table.database_name);
            tuple.push_string(&table.name);
            tuple.push_string(&column.name.to_string());
            tuple.push_string(
                &format!(
                    "{}_{}_{}_primary_key",
                    table.database_name,
                    table.name,
                    column.name.to_string()
                )
            );
            tuple.push_unsigned_bigint(1u64);
            tuples.push(tuple);

            let table_sequences = Table::new(
                Config::system_database(),
                Config::system_database_table_sequences()
            );

            insert_tuples(machine, &table_sequences, &mut tuples);
        }
    }
    Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE TABLE")))
}
