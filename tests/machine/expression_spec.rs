use rusticodb::machine::Column;
use rusticodb::machine::ColumnType;
use rusticodb::machine::RawVal;
use rusticodb::machine::Expression;
use rusticodb::machine::Expression1Type;
use rusticodb::machine::Expression2Type;

use rusticodb::storage::Cell;
use rusticodb::storage::CellType;
use rusticodb::storage::Tuple;

#[test]
pub fn test_if_expression_equal_operator() {
    let expression = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Int(1u64)))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), true);
}

#[test]
pub fn test_if_expression_equal_operator_with_string() {
    let expression = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Str(String::from("value1"))))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::Varchar,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_string(&String::from("value1"));
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), true);
}

#[test]
pub fn test_if_expression_equal_operator_with_null() {
    let expression = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Null))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::Varchar,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_null();
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), true);
}

#[test]
pub fn test_if_expression_not_equal_operator() {
    let expression = Expression::Func2(
        Expression2Type::NotEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Int(1u64)))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), false);
}

#[test]
pub fn test_if_expression_not_equal_operator_with_string() {
    let expression = Expression::Func2(
        Expression2Type::NotEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Str(String::from("value1"))))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::Varchar,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_string(&String::from("value1"));
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), false);
}

#[test]
pub fn test_if_expression_not_equal_operator_with_null() {
    let expression = Expression::Func2(
        Expression2Type::NotEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Null))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::Varchar,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_null();
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), false);
}

#[test]
pub fn test_if_expression_and_operator() {
    let expression = Expression::Func2(
        Expression2Type::And,
        Box::new( Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("id"))),
            Box::new(Expression::Const(RawVal::Int(1u64)))
        )),
        Box::new( Expression::Func2(
            Expression2Type::NotEqual,
            Box::new(Expression::ColName(String::from("id"))),
            Box::new(Expression::Const(RawVal::Int(2u64)))
        ))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), true);
}

#[test]
pub fn test_if_expression_or_operator() {
    let expression = Expression::Func2(
        Expression2Type::Or,
        Box::new( Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("id"))),
            Box::new(Expression::Const(RawVal::Int(1u64)))
        )),
        Box::new( Expression::Func2(
            Expression2Type::NotEqual,
            Box::new(Expression::ColName(String::from("id"))),
            Box::new(Expression::Const(RawVal::Int(2u64)))
        ))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), true);
}

#[test]
pub fn test_if_expression_greather_or_equal_operator_with_equal_value() {
    let expression = Expression::Func2(
        Expression2Type::GreatherOrEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Int(1u64)))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), true);
}

#[test]
pub fn test_if_expression_less_or_equal_operator_with_equal_value() {
    let expression = Expression::Func2(
        Expression2Type::LessOrEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Int(1u64)))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), true);
}

#[test]
pub fn test_if_expression_greather_or_equal_operator_with_diff_value() {
    let expression = Expression::Func2(
        Expression2Type::GreatherOrEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Int(1u64)))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(100u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), true);
}

#[test]
pub fn test_if_expression_less_or_equal_operator_with_diff_value() {
    let expression = Expression::Func2(
        Expression2Type::LessOrEqual,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Int(100u64)))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), true);
}

#[test]
pub fn test_if_expression_greather_than_operator() {
    let expression = Expression::Func2(
        Expression2Type::GreatherThan,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Int(1u64)))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(20u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), true);
}

#[test]
pub fn test_if_expression_less_than_operator() {
    let expression = Expression::Func2(
        Expression2Type::LessThan,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Int(100u64)))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(20u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), true);
}

#[test]
pub fn test_if_expression_add_operator() {
    let expression = Expression::Func2(
        Expression2Type::Add,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Int(100u64)))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(20u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::UnsignedBigint);
    assert_eq!(cell.bin_to_unsigned_bigint().unwrap(), 120u64);
}

#[test]
pub fn test_if_expression_add_operator_inverse() {
    let expression = Expression::Func2(
        Expression2Type::Add,
        Box::new(Expression::Const(RawVal::Int(100u64))),
        Box::new(Expression::ColName(String::from("id")))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(20u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::UnsignedBigint);
    assert_eq!(cell.bin_to_unsigned_bigint().unwrap(), 120u64);
}

#[test]
pub fn test_if_expression_sub_operator() {
    let expression = Expression::Func2(
        Expression2Type::Sub,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Int(100u64)))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(200u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::UnsignedBigint);
    assert_eq!(cell.bin_to_unsigned_bigint().unwrap(), 100u64);
}

#[test]
pub fn test_if_expression_sub_operator_inverse() {
    let expression = Expression::Func2(
        Expression2Type::Sub,
        Box::new(Expression::Const(RawVal::Int(200u64))),
        Box::new(Expression::ColName(String::from("id")))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(100u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::UnsignedBigint);
    assert_eq!(cell.bin_to_unsigned_bigint().unwrap(), 100u64);
}

#[test]
pub fn test_if_expression_mul_operator() {
    let expression = Expression::Func2(
        Expression2Type::Mul,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Int(100u64)))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(200u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::UnsignedBigint);
    assert_eq!(cell.bin_to_unsigned_bigint().unwrap(), 20000u64);
}


#[test]
pub fn test_if_expression_mul_operator_inverse() {
    let expression = Expression::Func2(
        Expression2Type::Mul,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Int(400u64)))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(200u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::UnsignedBigint);
    assert_eq!(cell.bin_to_unsigned_bigint().unwrap(), 80000u64);
}

#[test]
pub fn test_if_expression_div_operator() {
    let expression = Expression::Func2(
        Expression2Type::Div,
        Box::new(Expression::ColName(String::from("id"))),
        Box::new(Expression::Const(RawVal::Int(100u64)))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(200u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::UnsignedBigint);
    assert_eq!(cell.bin_to_unsigned_bigint().unwrap(), 2u64);
}


#[test]
pub fn test_if_expression_div_operator_inverse() {
    let expression = Expression::Func2(
        Expression2Type::Div,
        Box::new(Expression::Const(RawVal::Int(400u64))),
        Box::new(Expression::ColName(String::from("id")))
    );

    let columns = vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
    ];

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(200u64);
    
    let cell: Cell = expression.result(&tuple, &columns);

    assert_eq!(cell.get_type(), CellType::UnsignedBigint);
    assert_eq!(cell.bin_to_unsigned_bigint().unwrap(), 2u64);
}

#[test]
pub fn test_if_expression_not_operator_inverse() {
    let expression = Expression::Func1(
        Expression1Type::Not,
        Box::new(Expression::Const(RawVal::Int(1u64))),
    );

    let cell: Cell = expression.result(&Tuple::new(), &Vec::new());

    assert_eq!(cell.get_type(), CellType::Boolean);
    assert_eq!(cell.bin_to_boolean().unwrap(), false);
}

#[test]
pub fn test_if_expression_negate_operator_inverse() {
    let expression = Expression::Func1(
        Expression1Type::Negate,
        Box::new(Expression::Const(RawVal::Int(1u64))),
    );

    let cell: Cell = expression.result(&Tuple::new(), &Vec::new());

    assert_eq!(cell.get_type(), CellType::SignedBigint);
    assert_eq!(cell.bin_to_signed_bigint().unwrap(), -1);
}
