extern crate sqlparser;
use failure::Fail;
use sqlparser::ast::{Expr as ASTNode, *};

use crate::machine::column::Column;
use crate::machine::column::ColumnType;

#[derive(Debug)]
pub struct Query {
    pub select: Vec<Column>,
    pub table: String,
    // pub filter: Expr,
    //  pub order_by: Vec<(Expr, bool)>,
    //  pub limit: LimitClause,
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

// Convert sqlparser-rs `ASTNode` to LocustDB's `Query`
pub fn parse_query(query: Box<sqlparser::ast::Query>) -> Result<Query, QueryError> {
    //   let (projection, relation, selection, order_by, limit, offset) = get_query_components(query)?;
    //   let projection = get_projection(projection)?;
    //    let table = get_table_name(relation)?;
    //    let filter = match selection {
    //        Some(ref s) => *convert_to_native_expr(s)?,
    //        None => Expr::Const(RawVal::Int(1)),
    //    };
    //    let order_by = get_order_by(order_by)?;
    //    let limit_clause = LimitClause {
    //        limit: get_limit(limit)?,
    //        offset: get_offset(offset)?,
    //    };

    let (projection, relation, _selection) = get_query_components(query)?;
    let table_name = get_table_name(relation)?;
    let columns = get_projection(projection)?;

    let mut select: Vec<Column> = Vec::new();

    for item in columns {
        select.push(Column::new_column(item.clone(), ColumnType::Varchar));
    } 

    Ok(Query {
        select: select,
        table: table_name,
    })
}

type QueryResult = (Vec<SelectItem>, Option<TableFactor>, Option<ASTNode>);

// TODO: use struct
#[allow(clippy::type_complexity)]
fn get_query_components(query: Box<sqlparser::ast::Query>) -> Result<QueryResult, QueryError> {
    match *query.body {
        SetExpr::Select(select) => {
            let mut from = select.from;
            Ok((
                select.projection,
                from.pop().map(|t| t.relation),
                select.selection,
            ))
        }
        // TODO: more specific error messages
        _ => Err(QueryError::NotImplemented(
            "Only SELECT queries are supported.".to_string(),
        )),
    }
}

fn get_table_name(relation: Option<TableFactor>) -> Result<String, QueryError> {
    match relation {
        // TODO: error message if any unused fields are set
        Some(TableFactor::Table { name, .. }) => Ok(strip_quotes(&format!("{}", name))),
        Some(s) => Err(QueryError::ParseError(format!(
            "Invalid expression for table name: {:?}",
            s
        ))),
        None => Err(QueryError::ParseError("Table name missing.".to_string())),
    }
}

fn strip_quotes(ident: &str) -> String {
    if ident.starts_with('`') || ident.starts_with('"') {
        ident[1..ident.len() - 1].to_string()
    } else {
        ident.to_string()
    }
}

fn get_projection(projection: Vec<SelectItem>) -> Result<Vec<String>, QueryError> {
    let mut result = Vec::<String>::new();
    for elem in &projection {
        match elem {
            SelectItem::UnnamedExpr(e) => {
                // sqlparser-rs provides string of the projection as entered by the user.
                // Storing this string in Query.select corresponding to locustdb's Expr.
                // These will later be used as colnames of query results.
                result.push(strip_quotes(&format!("{}", e)));
            }
            SelectItem::Wildcard(_) => result.push("*".to_string()),
            SelectItem::ExprWithAlias { expr: _, alias } => result.push(strip_quotes(&alias.to_string())),
            _ => {
                return Err(QueryError::NotImplemented(format!(
                    "Unsupported projection in SELECT: {}",
                    elem
                )))
            }
        }
    }

    Ok(result)
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
