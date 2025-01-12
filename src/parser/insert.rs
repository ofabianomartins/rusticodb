extern crate sqlparser;

use sqlparser::ast::Insert;
use sqlparser::ast::Ident;
use sqlparser::ast::Query;
use sqlparser::ast::SetExpr;
use sqlparser::ast::Expr;
use sqlparser::ast::Value;

use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::ResultSet;
use crate::machine::Column;
use crate::machine::ColumnType;
use crate::machine::check_table_exists;
use crate::machine::insert_row;

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

pub fn insert(machine: &mut Machine, insert: Insert) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let table_name = insert.table_name.to_string();
        let table = Table::new(db_name.clone(), table_name.clone());

        let columns = get_columns(machine, insert.columns, &table);

        if check_table_exists(machine, &table) == false {
            return Err(ExecutionError::TableNotExists(table_name.to_string()));
        }

        let mut tuples = get_tuples(&columns, insert.source);

        return insert_row(machine, &table, &columns, &mut tuples);
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

