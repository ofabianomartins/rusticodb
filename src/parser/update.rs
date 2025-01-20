use sqlparser::ast::Ident;
use sqlparser::ast::Query;
use sqlparser::ast::SetExpr;
use sqlparser::ast::Expr;
use sqlparser::ast::Value;
use sqlparser::ast::Assignment;
use sqlparser::ast::TableWithJoins;
use sqlparser::ast::SelectItem;

use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::Column;
use crate::machine::ColumnType;

use crate::storage::Tuple;

use crate::utils::ExecutionError;

fn get_tuples(_columns: &Vec<Column>, source: Option<Box<Query>>) -> Vec<Tuple> {
    let mut tuples: Vec<Tuple> = Vec::new();

    if let Some(query) = source {
        let rows = (*query).body;
        match *rows {
            SetExpr::Values(values) => {
              for items in values.rows {
                let mut tuple = Tuple::new();
                for item in items {
                    match item {
                        Expr::Identifier(value) => {
                           tuple.push_string(&value.value);
                        },
                        Expr::Value(value) => {
                           match value {
                               Value::Null => {
                                   tuple.push_null();
                               },
                               _ => {}
                           }
                        },
                        _ => {}
                    }
                }
                tuples.push(tuple)
              }
            },
            _ => {}
        }
    }

    return tuples; 
}

pub fn get_columns(
    _machine: &mut Machine,
    query_columns: Vec<Ident>,
    table: &Table
) -> Vec<Column> {
    let mut columns = Vec::<Column>::new();

    for ident in &query_columns {
        columns.push(
            Column::new(
                table.database_name.clone(),
                table.name.clone(),
                ident.value.clone(),
                ColumnType::Varchar,
                false,
                false,
                false
            )
        )
    }
    return columns;
}

pub fn update(
    machine: &mut Machine,
    _table: TableWithJoins,
    _assignments: Vec<Assignment>,
    _selection: Option<Expr>,
    _returning: Option<Vec<SelectItem>>
) -> Result<ResultSet, ExecutionError> { 
    if let Some(_db_name) = machine.actual_database.clone() {
       return Ok(ResultSet::new_command(ResultSetType::Change, String::from("UPDATE")))
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

