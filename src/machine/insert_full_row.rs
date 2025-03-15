use crate::machine::Column;
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::get_sequence_next_id;

use crate::storage::Tuple;
use crate::storage::format_table_name;
use crate::storage::pager_insert_tuples;
use crate::storage::pager_flush_page;
use crate::storage::tuple_append_cell;
use crate::storage::tuple_get_cell;
use crate::storage::tuple_push_unsigned_bigint;
use crate::storage::tuple_new;

use crate::utils::ExecutionError;
use crate::utils::Logger;

pub fn insert_full_row(
    machine: &mut Machine,
    table: &Table,
    table_columns: &Vec<Column>,
    columns: &Vec<Column>,
    tuples: &mut Vec<Tuple>
) -> Result<ResultSet, ExecutionError>{
    let adjusted_tuples_result = adjust_tuples(machine, table_columns, columns, tuples);
    if let Err(error) = adjusted_tuples_result {
        return Err(error);
    }

    let mut adjusted_tuples = adjusted_tuples_result.unwrap();

    let page_key = format_table_name(&table.database_name, &table.name);
    pager_insert_tuples(&mut machine.pager, &page_key, &mut adjusted_tuples);
    pager_flush_page(&mut machine.pager, &page_key);

    return Ok(ResultSet::new_command(ResultSetType::Change, String::from("INSERT")))
}

fn adjust_tuples(
    machine: &mut Machine,
    table_columns: &Vec<Column>,
    columns: &Vec<Column>,
    tuples: &mut Vec<Tuple>
) -> Result<Vec<Tuple>, ExecutionError> {
    let new_tuples: Vec<Tuple> = tuples.iter_mut()
        .map(|tuple| { 
            let mut new_tuple = tuple_new();

            for (_idx, column) in table_columns.iter().enumerate() {
                let index_result = columns.iter().position(|e| e == column);
                if let Some(index) = index_result {
                    tuple_append_cell(&mut new_tuple, tuple_get_cell(tuple, index as u16));
                } else {
                    Logger::debug(format!("Getting next id for column {}", column).leak());
                    if let Some(next_id) = get_sequence_next_id(machine, column) {
                        Logger::debug(format!("Next id for column {} is {}", column, next_id).leak());
                        tuple_push_unsigned_bigint(&mut new_tuple, next_id);
                    }
                }
            }

            new_tuple
        })
        .collect::<Vec<_>>();

    return Ok(new_tuples);
}
