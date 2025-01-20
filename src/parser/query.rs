extern crate sqlparser;

use failure::Fail;

use sqlparser::ast::Query as Select;
use sqlparser::ast::{Expr as ASTNode, *};

use crate::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::machine::Table;
use crate::machine::Column;
use crate::machine::ColumnType;
use crate::machine::raw_val::RawVal;
use crate::machine::get_columns as machine_get_columns;
use crate::machine::check_table_exists;
use crate::machine::product_cartesian;
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

fn get_table_name(db_name: String, relations: Vec<TableWithJoins>) -> Vec<Table> {
    let mut result: Vec<Table> = Vec::new();
    for relation in relations {
        match relation.relation {
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
                result.push(table)
            },
            _ => {}
        }
    }
    return result;
}

pub fn get_columns(
    machine: &mut Machine,
    projection: Vec<SelectItem>,
    tables: Vec<Table>
) -> Vec<Column> {
    let mut columns = Vec::<Column>::new();
    for elem in &projection {
        match elem {
            SelectItem::UnnamedExpr(e) => {
                columns.push(
                    Column::new(
                        tables[0].database_name.clone(),
                        String::from(""),
                        e.to_string(),
                        ColumnType::Undefined,
                        false,
                        false,
                        false,
                    )
                )
            },
            SelectItem::ExprWithAlias { expr, alias } => {
                columns.push(
                    Column::new_with_alias(
                        tables[0].database_name.clone(),
                        tables[0].database_alias.clone(),
                        tables[0].name.clone(),
                        tables[0].alias.clone(),
                        expr.to_string(),
                        alias.to_string(),
                        ColumnType::Undefined,
                        false,
                        false,
                        false
                    )
                )
            },
            SelectItem::Wildcard(_) => {
                for table in &tables {
                    let table_columns = machine_get_columns(machine, table);
                    for column in table_columns {
                        columns.push(column);
                    }
                }
            },
            SelectItem::QualifiedWildcard(name, _options) => {
                for table in &tables {
                    if table.name == name.to_string() || table.alias == name.to_string() { 
                        for line in machine_get_columns(machine, &tables[0]) { 
                            columns.push(line);
                        }
                    }
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

pub fn query(machine: &mut Machine, query: Box<Select>) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let (projection, relations, selection, limit, offset) = get_query_components(query).unwrap();
        let tables: Vec<Table> = get_table_name(db_name.clone(), relations);

        for table in &tables {
            if check_table_exists(machine, &table) == false {
                return Err(ExecutionError::TableNotExists(table.name.to_string()));
            }
        }

        let columns = get_columns(machine, projection, tables.clone());

        let mut result_set = product_cartesian(machine, tables);
        result_set = result_set.projection(columns).unwrap();

        if let Ok(limit_size) = get_limit(limit) {
          result_set = result_set.limit(limit_size as usize);
        }

        if let Ok(offset_size) = get_offset(offset) {
          result_set = result_set.offset(offset_size as usize);
        }

        if let Some(selection_value) = &selection {
            if let Ok(condition) = convert_to_native_expr(selection_value) {
              result_set = result_set.selection(condition).unwrap();
            }
        }

        return Ok(result_set)
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

