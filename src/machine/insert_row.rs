
use crate::machine::Column;
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::get_sequence_next_id;
use crate::machine::get_columns;

use crate::storage::Tuple;
use crate::storage::CellType;

use crate::utils::execution_error::ExecutionError;

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

    machine.pager.insert_tuples(&table.database_name, &table.name, &mut adjusted_tuples);
    machine.pager.flush_page(&table.database_name, &table.name);

    return Ok(ResultSet::new_command(ResultSetType::Change, String::from("INSERT")))
}

pub fn validate_tuples(
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
                if column.not_null == true && tuple.get_cell(index as u16).data[0] == (CellType::Null as u8) {
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

    let new_tuples: Vec<Tuple> = tuples.iter_mut()
        .map(|tuple| { 
            let mut new_tuple = Tuple::new();

            for (_idx, column) in table_columns.iter().enumerate() {
                let index_result = columns.iter().position(|e| e == column);
                if let Some(index) = index_result {
                    if column.primary_key == true {
                       new_tuple.append_cell(tuple.get_cell(index as u16));
                    } else {
                        if let Some(next_id) = get_sequence_next_id(machine, column) {
                            new_tuple.push_unsigned_bigint(next_id);
                        }
                    }
                }
            }

            new_tuple
        })
        .collect::<Vec<_>>();

    return Ok(new_tuples);
}
