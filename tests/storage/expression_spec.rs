use rusticodb::machine::Column;
use rusticodb::machine::ColumnType;

use rusticodb::storage::Data;
use rusticodb::storage::Expression;
use rusticodb::storage::Expression1Type;
use rusticodb::storage::Expression2Type;
use rusticodb::storage::tuple_new;

#[test]
pub fn test_if_expression_equal_operator() {
    let expression = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::UnsignedBigint(1u64)))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(1u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_equal_operator_with_string() {
    let expression = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::Varchar(String::from("value1"))))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::Varchar("".to_string()),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::Varchar(String::from("value1")));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_equal_operator_with_null() {
    let expression = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::Null))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::Varchar("".to_string()),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::Null);
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_not_equal_operator() {
    let expression = Expression::Func2(
        Expression2Type::NotEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::UnsignedBigint(1u64)))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(1u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_not_equal_operator_with_string() {
    let expression = Expression::Func2(
        Expression2Type::NotEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::Varchar(String::from("value1"))))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::Varchar("".to_string()),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::Varchar(String::from("value1")));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_not_equal_operator_with_null() {
    let expression = Expression::Func2(
        Expression2Type::NotEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::Null))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::Varchar("".to_string()),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::Null);
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_and_operator() {
    let expression = Expression::Func2(
        Expression2Type::And,
        Box::new( Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("id"))),
            Box::new(Expression::Const(Data::UnsignedBigint(1u64)))
        )),
        Box::new( Expression::Func2(
            Expression2Type::NotEqual,
            Box::new(Expression::ColName(String::from("id"))),
            Box::new(Expression::Const(Data::UnsignedBigint(2u64)))
        ))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(1u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_or_operator() {
    let expression = Expression::Func2(
        Expression2Type::Or,
        Box::new( Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("id"))),
            Box::new(Expression::Const(Data::UnsignedBigint(1u64)))
        )),
        Box::new( Expression::Func2(
            Expression2Type::NotEqual,
            Box::new(Expression::ColName(String::from("id"))),
            Box::new(Expression::Const(Data::UnsignedBigint(2u64)))
        ))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(1u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_greather_or_equal_operator_with_equal_value() {
    let expression = Expression::Func2(
        Expression2Type::GreatherOrEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::UnsignedBigint(1u64)))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(1u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_less_or_equal_operator_with_equal_value() {
    let expression = Expression::Func2(
        Expression2Type::LessOrEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::UnsignedBigint(1u64)))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(1u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_greather_or_equal_operator_with_diff_value() {
    let expression = Expression::Func2(
        Expression2Type::GreatherOrEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::UnsignedBigint(1u64)))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(100u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_less_or_equal_operator_with_diff_value() {
    let expression = Expression::Func2(
        Expression2Type::LessOrEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::UnsignedBigint(100u64)))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(1u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_greather_than_operator() {
    let expression = Expression::Func2(
        Expression2Type::GreatherThan,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::UnsignedBigint(1u64)))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(20u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_less_than_operator() {
    let expression = Expression::Func2(
        Expression2Type::LessThan,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::UnsignedBigint(100u64)))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(20u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_add_operator() {
    let expression = Expression::Func2(
        Expression2Type::Add,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::UnsignedBigint(100u64)))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(20u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::UnsignedBigint(_)));
}

#[test]
pub fn test_if_expression_add_operator_inverse() {
    let expression = Expression::Func2(
        Expression2Type::Add,
        Box::new(Expression::Const(Data::UnsignedBigint(100u64))),
        Box::new(Expression::ColName(String::from("id")))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(20u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::UnsignedBigint(_)));
}

#[test]
pub fn test_if_expression_sub_operator() {
    let expression = Expression::Func2(
        Expression2Type::Sub,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::UnsignedBigint(100u64)))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(200u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::UnsignedBigint(_)));
}

#[test]
pub fn test_if_expression_sub_operator_inverse() {
    let expression = Expression::Func2(
        Expression2Type::Sub,
        Box::new(Expression::Const(Data::UnsignedBigint(200u64))),
        Box::new(Expression::ColName(String::from("id")))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(100u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::UnsignedBigint(_)));
}

#[test]
pub fn test_if_expression_mul_operator() {
    let expression = Expression::Func2(
        Expression2Type::Mul,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::UnsignedBigint(100u64)))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(200u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::UnsignedBigint(_)));
}


#[test]
pub fn test_if_expression_mul_operator_inverse() {
    let expression = Expression::Func2(
        Expression2Type::Mul,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::UnsignedBigint(400u64)))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(200u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::UnsignedBigint(_)));
}

#[test]
pub fn test_if_expression_div_operator() {
    let expression = Expression::Func2(
        Expression2Type::Div,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(Data::UnsignedBigint(100u64)))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(200u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::UnsignedBigint(_)));
}


#[test]
pub fn test_if_expression_div_operator_inverse() {
    let expression = Expression::Func2(
        Expression2Type::Div,
        Box::new(Expression::Const(Data::UnsignedBigint(400u64))),
        Box::new(Expression::ColName(String::from("id")))
    );

    let columns = vec![
        Column::new(
            0u64,
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        ),
    ].iter().map(|e| e.name.clone()).collect();

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(200u64));
    
    let cell = expression.result(&tuple, &columns);

    assert!(matches!(cell, Data::UnsignedBigint(_)));
}

#[test]
pub fn test_if_expression_not_operator_inverse() {
    let expression = Expression::Func1(
        Expression1Type::Not,
        Box::new(Expression::Const(Data::UnsignedBigint(1u64))),
    );

    let cell = expression.result(&tuple_new(), &Vec::new());

    assert!(matches!(cell, Data::Boolean(_)));
}

#[test]
pub fn test_if_expression_negate_operator_inverse() {
    let expression = Expression::Func1(
        Expression1Type::Negate,
        Box::new(Expression::Const(Data::UnsignedBigint(1u64))),
    );

    let cell = expression.result(&tuple_new(), &Vec::new());

    assert!(matches!(cell, Data::SignedBigint(_)));
}
