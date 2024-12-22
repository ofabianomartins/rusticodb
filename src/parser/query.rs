extern crate sqlparser;

use failure::Fail;

use sqlparser::ast::Query as Select;
use sqlparser::ast::{Expr as ASTNode, *};

use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::utils::execution_error::ExecutionError;
use crate::machine::column::Column;
use crate::machine::column::ColumnType;

fn strip_quotes(ident: &str) -> String {
    if ident.starts_with('`') || ident.starts_with('"') {
        ident[1..ident.len() - 1].to_string()
    } else {
        ident.to_string()
    }
}

/*
fn get_order_by(order_by: Option<Vec<OrderByExpr>>) -> Result<Vec<(Expr, bool)>, QueryError> {
    let mut order = Vec::new();
    if let Some(sql_order_by_exprs) = order_by {
        for e in sql_order_by_exprs {
            order.push((*(convert_to_native_expr(&e.expr))?, !e.asc.unwrap_or(true)));
        }
    }
    Ok(order)
}


fn function_arg_to_expr(node: &FunctionArg) -> Result<&ASTNode, QueryError> {
    match node {
        FunctionArg::Named { name, .. } => Err(QueryError::NotImplemented(format!(
            "Named function arguments are not supported: {}",
            name
        ))),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
            Err(QueryError::NotImplemented("Wildcard function arguments are not supported".to_string()))
        }
        FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => {
            Err(QueryError::NotImplemented("Qualified wildcard function arguments are not supported".to_string()))
        }
    }
}

fn func_arg_to_native_expr(node: &FunctionArg) -> Result<Box<Expr>, QueryError> {
    convert_to_native_expr(function_arg_to_expr(node)?)
}

fn map_unary_operator(op: &UnaryOperator) -> Result<Func1Type, QueryError> {
    Ok(match op {
        UnaryOperator::Not => Func1Type::Not,
        UnaryOperator::Minus => Func1Type::Negate,
        _ => return Err(fatal!("Unexpected unary operator: {}", op)),
    })
}

fn map_binary_operator(o: &BinaryOperator) -> Result<Func2Type, QueryError> {
    Ok(match o {
        BinaryOperator::And => Func2Type::And,
        BinaryOperator::Plus => Func2Type::Add,
        BinaryOperator::Minus => Func2Type::Subtract,
        BinaryOperator::Multiply => Func2Type::Multiply,
        BinaryOperator::Divide => Func2Type::Divide,
        BinaryOperator::Modulo => Func2Type::Modulo,
        BinaryOperator::Gt => Func2Type::GT,
        BinaryOperator::GtEq => Func2Type::GTE,
        BinaryOperator::Lt => Func2Type::LT,
        BinaryOperator::LtEq => Func2Type::LTE,
        BinaryOperator::Eq => Func2Type::Equals,
        BinaryOperator::NotEq => Func2Type::NotEquals,
        BinaryOperator::Or => Func2Type::Or,
        _ => {
            return Err(QueryError::NotImplemented(format!(
                "Unsupported operator {:?}",
                o
            )))
        }
    })
}

// Fn to map sqlparser-rs `Value` to LocustDB's `RawVal`.
fn get_raw_val(constant: &Value) -> Result<RawVal, QueryError> {
    match constant {
        Value::Number(num, _) => {
            if num.parse::<i64>().is_ok() {
                Ok(RawVal::Int(num.parse::<i64>().unwrap()))
            } else {
                Ok(RawVal::Float(ordered_float::OrderedFloat(num.parse::<f64>().unwrap())))
            }
        },
        Value::SingleQuotedString(string) => Ok(RawVal::Str(string.to_string())),
        Value::Null => Ok(RawVal::Null),
        _ => Err(QueryError::NotImplemented(format!("{:?}", constant))),
    }
}

*/

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

type QueryResult = (Vec<SelectItem>, Vec<TableWithJoins>, Option<ASTNode>, Option<Expr>, Option<Offset>);

#[allow(clippy::type_complexity)]
fn get_query_components(query: Box<Select>) -> Result<QueryResult, QueryError> {
    match *query.body {
        SetExpr::Select(select) => {
            Ok((select.projection, select.from, select.selection, query.limit, query.offset))
        },
        _ => Err(
            QueryError::NotImplemented(
                "Only SELECT queries are supported.".to_string()
            )
        )
    }
}

fn get_table_name(relations: Vec<TableWithJoins>) -> Vec<String> {
    let mut result: Vec<String> = Vec::new();
    for relation in relations {
        match relation.relation {
            TableFactor::Table { name, .. } => {
                result.push(strip_quotes(&name.to_string().leak()))
            },
            _ => {}
        }
    }
    return result;
}

pub fn get_columns(
    machine: &mut Machine,
    projection: Vec<SelectItem>,
    db_name: String,
    table_names: Vec<String>
) -> Vec<Column> {
    let mut columns = Vec::<Column>::new();
    for elem in &projection {
        match elem {
            SelectItem::UnnamedExpr(e) => {
                columns.push(
                    Column::new(
                        db_name.clone(),
                        String::from(""),
                        e.to_string(),
                        ColumnType::Varchar
                    )
                )
            },
            SelectItem::ExprWithAlias { expr, alias } => {
                columns.push(
                    Column::new_with_alias(
                        db_name.clone(),
                        String::from(""),
                        expr.to_string(),
                        alias.to_string(),
                        ColumnType::Varchar
                    )
                )
            },
            SelectItem::Wildcard(_) => {
                for table_name in &table_names {
                    let list_column_from_table = machine.list_columns(
                        db_name.clone(), 
                        table_name.clone()
                    );
                    for line in list_column_from_table.tuples.clone().into_iter() {
                        columns.push(
                            Column::new(
                                db_name.clone(),
                                table_name.clone(),
                                line.get_string(2).unwrap(),
                                ColumnType::Varchar
                            )
                        )
                    }
                }
            },
            SelectItem::QualifiedWildcard(name, _options) => {
                let list_column_from_table = machine.list_columns(
                    db_name.clone(), 
                    name.to_string()
                );
                for line in list_column_from_table.tuples.clone().into_iter() {
                    columns.push(
                        Column::new(
                            db_name.clone(),
                            name.to_string(),
                            line.get_string(2).unwrap(),
                            ColumnType::Varchar
                        )
                    )
                }
            },
        }
    }
    return columns;
}

fn get_limit(limit: Option<ASTNode>) -> Result<u64, QueryError> {
    match limit {
        Some(ASTNode::Value(Value::Number(int, _))) => Ok(int.parse::<u64>().unwrap()),
        None => Ok(u64::MAX),
        _ => Err(QueryError::NotImplemented(format!(
            "Invalid expression in limit clause: {:?}",
            limit
        ))),
    }
}

fn get_offset(offset: Option<Offset>) -> Result<u64, QueryError> {
    match offset {
        None => Ok(0),
        Some(offset) => match offset.value {
            ASTNode::Value(Value::Number(rows, _)) => Ok(rows.parse::<u64>().unwrap()),
            expr => Err(QueryError::ParseError(format!(
                "Invalid expression in offset clause: Expected constant integer, got {:?}",
                expr,
            ))),
        },
    }
}

pub fn query(machine: &mut Machine, query: Box<Select>) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.context.actual_database.clone() {
        let (projection, relations, _selection, limit, offset) = get_query_components(query).unwrap();
        let table_names: Vec<String> = get_table_name(relations);

        for table_name in &table_names {
            if machine.context.check_table_exists(&db_name, &table_name.clone()) == false {
                return Err(ExecutionError::TableNotExists(table_name.to_string()));
            }
        }

        let columns = get_columns(machine, projection, db_name.clone(), table_names.clone());

        let mut result_set = machine.product_cartesian(&db_name, table_names);
        result_set = result_set.projection(columns).unwrap();

        if let Ok(limit_size) = get_limit(limit) {
          result_set = result_set.limit(limit_size as usize);
        }

        if let Ok(offset_size) = get_offset(offset) {
          result_set = result_set.offset(offset_size as usize);
        }
        
        return Ok(result_set)
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

