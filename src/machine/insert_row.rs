
use crate::machine::Column;
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::get_sequence_next_id;
use crate::machine::get_columns;

use crate::storage::Tuple;
use crate::storage::Data;
use crate::storage::format_table_name;
use crate::storage::pager_insert_tuples;
use crate::storage::pager_flush_page;
use crate::storage::tuple_new;
use crate::storage::ResultSet;
use crate::storage::ResultSetType;

use crate::utils::ExecutionError;

pub fn insert_row(
    machine: &mut Machine,
    table: &Table,
    columns: &Vec<Column>,
    tuples: &mut Vec<Tuple>
) -> Result<ResultSet, ExecutionError>{
    if let Err(error) = validate_tuples(machine, table, columns, tuples) {
        return Err(error);
    }

    let adjusted_tuples_result = adjust_tuples(machine, table, columns, tuples);
    if let Err(error) = adjusted_tuples_result {
        return Err(error);
    }

    let mut adjusted_tuples = adjusted_tuples_result.unwrap();

    let page_key = format_table_name(&table.database_name, &table.name);
    pager_insert_tuples(&mut machine.pager, &page_key, &mut adjusted_tuples);
    pager_flush_page(&mut machine.pager, &page_key);

    return Ok(ResultSet::new_command(ResultSetType::Change, String::from("INSERT")))
}

fn validate_tuples(
    machine: &mut Machine, 
    table: &Table,
    columns: &Vec<Column>, 
    tuples: &mut Vec<Tuple>
) -> Result<bool, ExecutionError> {
    let table_columns = get_columns(machine, table);

    for (_idx, tuple) in tuples.iter().enumerate() {
        for (_idx, column) in table_columns.iter().enumerate() {
            let index_result = columns.iter().position(|e| e == column);
            if let Some(index) = index_result {
                if column.not_null == true && Data::Null == tuple.get(index).unwrap().clone() {
                    return Err(ExecutionError::ColumnCantBeNull(
                            table.database_name.clone(),
                            table.name.clone(),
                            column.name.clone()
                    ));
                }
            }
        }
    }

    return Ok(true);
}

fn adjust_tuples(
    machine: &mut Machine,
    table: &Table,
    columns: &Vec<Column>,
    tuples: &mut Vec<Tuple>
) -> Result<Vec<Tuple>, ExecutionError> {
    let table_columns = get_columns(machine, table);

    let new_tuples: Vec<Tuple> = tuples.iter()
        .map(|tuple| { 
            let mut new_tuple = tuple_new();

            for (_idx, column) in table_columns.iter().enumerate() {
                let index_result = columns.iter().position(|e| e == column);
                if let Some(index) = index_result {
                    new_tuple.push(tuple.get(index).unwrap().clone());
                } else {
                    if let Some(next_id) = get_sequence_next_id(machine, column) {
                        new_tuple.push(Data::UnsignedBigint(next_id));
                    }
                }
            }

            new_tuple
        })
        .collect::<Vec<_>>();

    return Ok(new_tuples);
}
