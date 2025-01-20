extern crate sqlparser;

use failure::Fail;

use sqlparser::ast::Delete;
use sqlparser::ast::{Expr as ASTNode, *};

use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::Table;
use crate::machine::raw_val::RawVal;
use crate::machine::get_columns;
use crate::machine::check_table_exists;
use crate::machine::drop_tuples;
use crate::machine::Expression;
use crate::machine::Expression1Type;
use crate::machine::Expression2Type;

use crate::utils::ExecutionError;

fn strip_quotes(ident: &str) -> String {
    if ident.starts_with('`') || ident.starts_with('"') {
        ident[1..ident.len() - 1].to_string()
    } else {
        ident.to_string()
    }
}

#[derive(Fail, Debug)]
pub enum QueryError {
    #[fail(display = "Failed to parse query. Chars remaining: {}", _0)]
    SytaxErrorCharsRemaining(String),
    #[fail(display = "Failed to parse query. Bytes remaining: {:?}", _0)]
    SyntaxErrorBytesRemaining(Vec<u8>),
    #[fail(display = "Failed to parse query: {}", _0)]
    ParseError(String),
    // #[fail(display = "Some assumption was violated. This is a bug: {}", _0)]
    // FatalError(String, Backtrace),
    #[fail(display = "Not implemented: {}", _0)]
    NotImplemented(String),
    #[fail(display = "Type error: {}", _0)]
    TypeError(String),
    #[fail(display = "Overflow or division by zero")]
    Overflow,
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
fn get_raw_val(constant: &Value) -> Result<RawVal, QueryError> {
    match constant {
        Value::Number(num, _) => {
            if num.parse::<u64>().is_ok() {
                Ok(RawVal::Int(num.parse::<u64>().unwrap()))
            } else {
                Ok(RawVal::Float(ordered_float::OrderedFloat(num.parse::<f64>().unwrap())))
            }
        },
        Value::SingleQuotedString(string) => Ok(RawVal::Str(string.to_string())),
        Value::Null => Ok(RawVal::Null),
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

