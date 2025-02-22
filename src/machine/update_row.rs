use crate::machine::Column;
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::get_sequence_next_id;
use crate::machine::get_columns;
use crate::machine::Expression;
use crate::machine::Attribution;
use crate::machine::read_tuples;
use crate::machine::update_tuples;

use crate::storage::Tuple;
use crate::storage::CellType;

use crate::utils::ExecutionError;

pub fn update_row(
    machine: &mut Machine,
    table: &Table,
    attributions: &Vec<Attribution>,
    expressions: Expression
) -> Result<ResultSet, ExecutionError> {
    let mut original_tuples = read_tuples(machine, table);

    let columns = get_columns(machine, table);

    let updated_tuples_result = adjust_tuples(
        machine,
        table,
        &columns,
        &mut original_tuples,
        attributions,
        expressions
    );
    if let Ok(mut new_tuples) = updated_tuples_result {
        update_tuples(machine, table, &mut new_tuples);
        
        return Ok(
            ResultSet::new_command(
               ResultSetType::Change,
               String::from("UPDATE")
            )
        )
    }

    if let Err(error) = updated_tuples_result {
        return Err(error);
    }
    return Err(ExecutionError::FailedUpdateTuples);
}


fn adjust_tuples(
    machine: &mut Machine,
    table: &Table,
    columns: &Vec<Column>,
    tuples: &mut Vec<Tuple>,
    attributions: &Vec<Attribution>,
    expressions: Expression
) -> Result<Vec<Tuple>, ExecutionError> {
    let table_columns = get_columns(machine, table);

    let new_tuples: Vec<Tuple> = tuples.iter_mut()
        .map(|tuple| { 
            let mut new_tuple = Tuple::new();

            for (idx, column) in table_columns.iter().enumerate() {
                let index_result = attributions.iter().position(|e| e.target == *column);
                if let Some(index) = index_result {
                    let attr = attributions.get(index).unwrap();
                    new_tuple.append_cell(attr.expr.result(&tuple, &table_columns));
                } else {
                    new_tuple.append_cell(tuple.get_cell(idx as u16));
                } 
            }

            new_tuple
        })
        .collect::<Vec<_>>();

    return Ok(new_tuples);
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
                if column.not_null == true && tuple.get_cell(index as u16).data[0] == (CellType::Null as u8) {
                    return Err(ExecutionError::ColumnCantBeNull(
                            table.database_name.clone(),
                            table.name.clone(),
                            column.name.clone()
                    )); } }
        }
    }

    return Ok(true);
}
