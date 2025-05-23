extern crate sqlparser;

use sqlparser::ast::Delete;
use sqlparser::ast::{Expr as ASTNode, *};

use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::get_columns;
use crate::machine::check_table_exists;
use crate::machine::drop_tuples;

use crate::storage::Data;
use crate::storage::Expression;
use crate::storage::Expression1Type;
use crate::storage::Expression2Type;
use crate::storage::ResultSet;
use crate::storage::ResultSetType;

use crate::utils::ExecutionError;
use crate::utils::QueryError;

fn strip_quotes(ident: &str) -> String {
    if ident.starts_with('`') || ident.starts_with('"') {
        ident[1..ident.len() - 1].to_string()
    } else {
        ident.to_string()
    }
}

fn map_binary_operator(o: &BinaryOperator) -> Result<Expression2Type, QueryError> {
    Ok(match o {
        BinaryOperator::And => Expression2Type::And,
        BinaryOperator::Or => Expression2Type::Or,
        BinaryOperator::Eq => Expression2Type::Equal,
        BinaryOperator::NotEq => Expression2Type::NotEqual,
//        BinaryOperator::Plus => Func2Type::Add,
//        BinaryOperator::Minus => Func2Type::Subtract,
//        BinaryOperator::Multiply => Func2Type::Multiply,
//        BinaryOperator::Divide => Func2Type::Divide,
//        BinaryOperator::Modulo => Func2Type::Modulo,
//        BinaryOperator::Gt => Func2Type::GT,
//        BinaryOperator::GtEq => Func2Type::GTE,
//        BinaryOperator::Lt => Func2Type::LT,
//        BinaryOperator::LtEq => Func2Type::LTE,
        _ => {
            return Err(QueryError::NotImplemented(format!(
                "Unsupported operator {:?}",
                o
            )))
        }
    })
}

fn map_unary_operator(op: &UnaryOperator) -> Result<Expression1Type, QueryError> {
    Ok(match op {
        UnaryOperator::Not => Expression1Type::Not,
        UnaryOperator::Minus => Expression1Type::Negate,
        _ => {
            return Err(QueryError::NotImplemented(format!(
                "Unsupported operator {:?}",
                op
            )))
        }
    })
}

// Fn to map sqlparser-rs `Value` to LocustDB's `RawVal`.
fn get_raw_val(constant: &Value) -> Result<Data, QueryError> {
    match constant {
        Value::Number(num, _) => {
            if num.parse::<u64>().is_ok() {
                Ok(Data::UnsignedBigint(num.parse::<u64>().unwrap()))
            } else {
                // Ok(RawVal::Float(ordered_float::OrderedFloat(num.parse::<f64>().unwrap())))
                Ok(Data::Null) 
            }
        },
        Value::SingleQuotedString(string) => Ok(Data::Varchar(string.to_string())),
        Value::Null => Ok(Data::Null),
        _ => Err(QueryError::NotImplemented(format!("{:?}", constant))),
    }
}

fn convert_to_native_expr(node: &ASTNode) -> Result<Expression, QueryError> {
    Ok(match node {
        ASTNode::BinaryOp {
            ref left,
            ref op,
            ref right,
        } => Expression::Func2(
            map_binary_operator(op)?,
            Box::new(convert_to_native_expr(left)?),
            Box::new(convert_to_native_expr(right)?)
        ),
        ASTNode::UnaryOp {
            ref op,
            expr: ref expression,
        } => Expression::Func1(map_unary_operator(op)?, Box::new(convert_to_native_expr(expression)?)),
        ASTNode::Value(ref literal) => Expression::Const(get_raw_val(literal)?),
        ASTNode::Identifier(ref identifier) => {
            Expression::ColName(strip_quotes(identifier.value.as_ref()))
        }
        _ => {
            println!("Parsing for this ASTNode not implemented: {:?}", node);
            return Err(QueryError::NotImplemented(format!("Parsing for this ASTNode not implemented: {:?}", node)))
        }
    })
}

fn get_table_name(db_name: String, relation: FromTable) -> Result<Table, ExecutionError> {
    let table: Table;
    match relation {
        FromTable::WithFromKeyword(table_with_joins) => {
            match &table_with_joins[0].relation {
                TableFactor::Table { name, alias, .. } => {
                    if let Some(alias_name) = alias {
                        table = Table::new_with_alias(
                            db_name.clone(),
                            db_name.clone(),
                            strip_quotes(&name.to_string().leak()),
                            strip_quotes(&alias_name.to_string().leak())
                        )
                    } else {
                        table = Table::new(
                            db_name.clone(),
                            strip_quotes(&name.to_string().leak())
                        )
                    }
                    Ok(table)
                },
                _ => Err(ExecutionError::TableNotExists(String::from("Table name not given")))
            }
        },
        FromTable::WithoutKeyword(table_with_joins) => {
            match &table_with_joins[0].relation {
                TableFactor::Table { name, alias, .. } => {
                    let table: Table;
                    if let Some(alias_name) = alias {
                        table = Table::new_with_alias(
                            db_name.clone(),
                            db_name.clone(),
                            strip_quotes(&name.to_string().leak()),
                            strip_quotes(&alias_name.to_string().leak())
                        )
                    } else {
                        table = Table::new(
                            db_name.clone(),
                            strip_quotes(&name.to_string().leak())
                        )
                    }
                    Ok(table)
                },
                _ => Err(ExecutionError::TableNotExists(String::from("Table name not given")))
            }
        }
    }
}

pub fn delete(machine: &mut Machine, query: Delete) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let table: Table;

        match get_table_name(db_name, query.from) {
            Ok(table_obj) => { table = table_obj },
            Err(err) => { return Err(err) }
        }

        if check_table_exists(machine, &table) == false {
            return Err(ExecutionError::TableNotExists(table.name.to_string()));
        }

        let columns = get_columns(machine, &table);

        let mut condition: Expression = Expression::Empty;

        if let Some(selection) = query.selection {
            condition = convert_to_native_expr(&selection).unwrap();
        }

        let _result_set = drop_tuples(machine, &table, columns, &condition);

        return Ok(ResultSet::new_command(ResultSetType::Change, String::from("DELETE ROWS")));
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

