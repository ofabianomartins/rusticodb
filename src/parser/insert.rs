extern crate sqlparser;

use sqlparser::ast::Insert;
use sqlparser::ast::Ident;
use sqlparser::ast::Query;
use sqlparser::ast::SetExpr;
use sqlparser::ast::Expr;
use sqlparser::ast::Value;

use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::Column;
use crate::machine::ColumnType;
use crate::machine::check_table_exists;
use crate::machine::insert_row;
use crate::machine::get_columns;
use crate::machine::get_sequence_next_id;

use crate::storage::Tuple;
use crate::storage::Data;
use crate::storage::tuple_new;
use crate::storage::ResultSet;

use crate::utils::ExecutionError;

fn get_tuples(
    machine: &mut Machine,
    table: &Table,
    columns: &Vec<Column>,
    query_columns: Vec<Ident>,
    source: Option<Box<Query>>
) -> Result<Vec<Tuple>, ExecutionError> {
    let mut tuples: Vec<Tuple> = Vec::new();

    if let Some(query) = source {
        let rows = (*query).body;
        match *rows {
            SetExpr::Values(values) => {
              for items in values.rows {
                let mut tuple = tuple_new();

                for tcolumn in columns.iter() {
                    let column_position_option = query_columns.iter().position(|e| e.value == tcolumn.name);

                    if let Some(column_position) = column_position_option {
                        match items.get(column_position) {
                            Some(Expr::Identifier(_)) => {},
                            Some(Expr::Value(Value::Number(value, _))) => {
                                if tcolumn.clone().is_number() {
                                    let my_integer: Result<u64, _> = value.parse();
                                    match my_integer {
                                        Ok(number) => tuple.push(Data::UnsignedBigint(number)),
                                        Err(_) => println!("Failed to parse string to integer"),
                                    }
                                }
                            },
                            Some(Expr::Value(Value::Null)) => {
                                if tcolumn.not_null == false {
                                    tuple.push(Data::Null);
                                } else {
                                    return Err(ExecutionError::ColumnCantBeNull(
                                        table.database_name.clone(),
                                        table.name.clone(),
                                        tcolumn.name.clone()
                                    ))
                                }
                            },
                            Some(Expr::Value(Value::SingleQuotedString(value))) => {
                                if matches!(tcolumn.column_type,ColumnType::Varchar(_)) {
                                    tuple.push(Data::Varchar(value.clone()));
                                }
                            },
                            other => {
                                println!("inserted value not identified {:?}", other);
                            }
                        }
                    } else if tcolumn.primary_key {
                        if let Some(next_id) = get_sequence_next_id(machine, tcolumn) {
                            tuple.push(Data::UnsignedBigint(next_id));
                        }
                    } else if tcolumn.not_null && tcolumn.default == String::from("") {
                        return Err(ExecutionError::ColumnCantBeNull(
                            table.database_name.clone(),
                            table.name.clone(),
                            tcolumn.name.clone()
                        ))
                    } else if tcolumn.default != String::from("") {
                        match &tcolumn.column_type {
                            ColumnType::UnsignedBigint(_) => {
                                let num_parse: u64 = tcolumn.default.parse::<u64>().unwrap();
                                tuple.push(Data::UnsignedBigint(num_parse));
                            },
                            ColumnType::UnsignedInt(_) => {
                                let num_parse: u32 = tcolumn.default.parse::<u32>().unwrap();
                                tuple.push(Data::UnsignedInt(num_parse));
                            },
                            ColumnType::UnsignedSmallint(_) => {
                                let num_parse: u16 = tcolumn.default.parse::<u16>().unwrap();
                                tuple.push(Data::UnsignedSmallint(num_parse));
                            },
                            ColumnType::UnsignedTinyint(_) => {
                                let num_parse: u8 = tcolumn.default.parse::<u8>().unwrap();
                                tuple.push(Data::UnsignedTinyint(num_parse));
                            },
                            ColumnType::SignedBigint(_) => {
                                let num_parse: i64 = tcolumn.default.parse::<i64>().unwrap();
                                tuple.push(Data::SignedBigint(num_parse));
                            },
                            ColumnType::SignedInt(_) => {
                                let num_parse: i32 = tcolumn.default.parse::<i32>().unwrap();
                                tuple.push(Data::SignedInt(num_parse));
                            },
                            ColumnType::SignedSmallint(_) => {
                                let num_parse: i16 = tcolumn.default.parse::<i16>().unwrap();
                                tuple.push(Data::SignedSmallint(num_parse));
                            },
                            ColumnType::SignedTinyint(_) => {
                                let num_parse: i8 = tcolumn.default.parse::<i8>().unwrap();
                                tuple.push(Data::SignedTinyint(num_parse));
                            },
                            ColumnType::Boolean(_) => {
                                let value = tcolumn.default == String::from("1");
                                tuple.push(Data::Boolean(value));
                            },
                            ColumnType::Varchar(_) => tuple.push(Data::Varchar(tcolumn.default.clone())),
                            ColumnType::Text(_) => tuple.push(Data::Text(tcolumn.default.clone())),
                            other => {
                                println!("insert value and column_type not identified {:?}", other);
                            }
                        }
                    } else if tcolumn.not_null == false {
                        tuple.push(Data::Null);
                    } 
                }

                tuples.push(tuple)
              }
            },
            _ => {}
        }
    }

    return Ok(tuples); 
}

pub fn insert(machine: &mut Machine, insert: Insert) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let table_name = insert.table_name.to_string();
        let table = Table::new(db_name.clone(), table_name.clone());

        if check_table_exists(machine, &table) == false {
            return Err(ExecutionError::TableNotExists(table_name.to_string()));
        }

        let columns = get_columns(machine, &table);

        let tuples_result = get_tuples(machine, &table, &columns, insert.columns, insert.source);

        match tuples_result {
           Ok(mut tuples) => insert_row(machine, &table, &columns, &columns, &mut tuples, false),
           Err(err) => Err(err)
        }
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

