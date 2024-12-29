extern crate sqlparser;

use sqlparser::ast::Insert;
use sqlparser::ast::Ident;
use sqlparser::ast::Query;
use sqlparser::ast::SetExpr;
use sqlparser::ast::Expr;

use crate::machine::machine::Machine;
use crate::machine::table::Table;
use crate::machine::result_set::ResultSet;
use crate::machine::result_set::ResultSetType;
use crate::storage::tuple::Tuple;
use crate::utils::execution_error::ExecutionError;

fn get_tuples(_columns: Vec<Ident>, source: Option<Box<Query>>) -> Vec<Tuple> {
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

pub fn insert(machine: &mut Machine, insert: Insert) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.context.actual_database.clone() {
        let table_name = insert.table_name.to_string();

        let table = Table::new(db_name.clone(), table_name.clone());
        if machine.context.check_table_exists(&table) == false {
            return Err(ExecutionError::TableNotExists(table_name.to_string()));
        }

        let mut tuples = get_tuples(insert.columns, insert.source);

        machine.insert_tuples(&table, &mut tuples);

        return Ok(ResultSet::new_command(ResultSetType::Change, String::from("INSERT")))
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

