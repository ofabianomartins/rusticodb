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
use crate::machine::get_columns;

use crate::storage::Tuple;

use crate::utils::ExecutionError;

fn get_tuples(source: Option<Box<Query>>) -> Vec<Tuple> {
    let mut tuples: Vec<Tuple> = Vec::new();

    if let Some(query) = source {
        let rows = (*query).body;
        match *rows {
            SetExpr::Values(values) => {
              for items in values.rows {
                let mut tuple = Tuple::new();
                for item in items {
                    match item {
                        Expr::Identifier(_) => {},
                        Expr::Value(Value::Number(value, _)) => {
                            let my_integer: Result<u64, _> = value.parse();
                            match my_integer {
                                Ok(number) => tuple.push_unsigned_bigint(number),
                                Err(_) => println!("Failed to parse string to integer"),
                            }
                        },
                        Expr::Value(Value::Null) => {
                            tuple.push_null();
                        },
                        Expr::Value(Value::SingleQuotedString(value)) => {
                            tuple.push_string(&value);
                        },
                        other => {
                            println!("value not identified {:?}", other);
                        }
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

pub fn get_insert_columns(
    machine: &mut Machine,
    table: &Table,
    query_columns: Vec<Ident>,
    source: &Option<Box<Query>>
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

    if columns.len() == 0 { 
        if let Some(query) = source {
            let rows = (*query).body.clone();
            match *rows {
                SetExpr::Values(values) => {
                  let items = values.rows.get(0);
                  if let Some(item) = items {
                    let table_columns = get_columns(machine, table);
                    let mut item_index = 0;
                    let mut column_index = 0;
                    
                    loop {
                        if table_columns.len() == column_index {
                            break;
                        }

                        let tcolumn = table_columns.get(column_index).unwrap();

                        match item.get(item_index) {
                            Some(Expr::Identifier(_)) => {},
                            Some(Expr::Value(Value::Number(_, _))) => {
                                if tcolumn.is_number() {
                                    columns.push(tcolumn.clone());
                                    item_index += 1;
                                    column_index += 1;
                                }
                            },
                            Some(Expr::Value(Value::Null)) => {
                                if tcolumn.not_null == false {
                                    columns.push(tcolumn.clone());
                                    item_index += 1;
                                    column_index += 1;
                                } else {
                                }
                            },
                            Some(Expr::Value(Value::SingleQuotedString(_))) => {
                                if tcolumn.column_type == ColumnType::Varchar {
                                    columns.push(tcolumn.clone());
                                    item_index += 1;
                                    column_index += 1;
                                }
                            },
                            other => {
                                println!(" test other {:?}", other);
                            }
                        }
                    }
                  }
                },
                _ => {}
            }
        }
    }
    return columns;
}

pub fn insert(machine: &mut Machine, insert: Insert) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let table_name = insert.table_name.to_string();
        let table = Table::new(db_name.clone(), table_name.clone());

        if check_table_exists(machine, &table) == false {
            return Err(ExecutionError::TableNotExists(table_name.to_string()));
        }

        let columns = get_insert_columns(machine, &table, insert.columns, &insert.source);

        let mut tuples = get_tuples(insert.source);

        return insert_row(machine, &table, &columns, &mut tuples);
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

