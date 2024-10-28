use rusticodb::machine::result_set::ResultSet;
use rusticodb::machine::column::{Column, ColumnType};
use rusticodb::storage::cell::ParserError;
use rusticodb::storage::tuple::Tuple;

#[test]
pub fn test_check_string_line_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_string(&String::from("database1"));
    tuples.push(tuple);

    let result_set = ResultSet::new_select(columns, tuples);

    assert!(matches!(result_set.get_string(0, &String::from("name")),Ok(_)));
    assert_eq!(result_set.line_count(), 1);
    assert_eq!(result_set.column_count(), 1);
}

#[test]
pub fn test_check_text_line_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_text(&String::from("database1"));
    tuples.push(tuple);

    let result_set = ResultSet::new_select(columns, tuples);

    assert!(matches!(result_set.get_text(0, &String::from("name")),Ok(_)));
    assert_eq!(result_set.line_count(), 1);
    assert_eq!(result_set.column_count(), 1);
}

#[test]
pub fn test_check_unsigned_tinyint_line_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_unsigned_tinyint(8u8);
    tuples.push(tuple);

    let result_set = ResultSet::new_select(columns, tuples);

    assert!(matches!(result_set.get_unsigned_tinyint(0, &String::from("name")),Ok(_)));
    assert_eq!(result_set.line_count(), 1);
    assert_eq!(result_set.column_count(), 1);
}

#[test]
pub fn test_check_unsigned_smallint_line_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_unsigned_smallint(8u16);
    tuples.push(tuple);

    let result_set = ResultSet::new_select(columns, tuples);

    assert!(matches!(result_set.get_unsigned_smallint(0, &String::from("name")),Ok(_)));
    assert_eq!(result_set.line_count(), 1);
    assert_eq!(result_set.column_count(), 1);
}

#[test]
pub fn test_check_unsigned_int_line_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_unsigned_int(8u32);
    tuples.push(tuple);

    let result_set = ResultSet::new_select(columns, tuples);

    assert!(matches!(result_set.get_unsigned_int(0, &String::from("name")),Ok(_)));
    assert_eq!(result_set.line_count(), 1);
    assert_eq!(result_set.column_count(), 1);
}

#[test]
pub fn test_check_unsigned_bigint_line_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(8u64);
    tuples.push(tuple);

    let result_set = ResultSet::new_select(columns, tuples);

    assert!(matches!(result_set.get_unsigned_bigint(0, &String::from("name")),Ok(_)));
    assert_eq!(result_set.line_count(), 1);
    assert_eq!(result_set.column_count(), 1);
}

#[test]
pub fn test_check_signed_tinyint_line_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_signed_tinyint(8i8);
    tuples.push(tuple);

    let result_set = ResultSet::new_select(columns, tuples);

    assert!(matches!(result_set.get_signed_tinyint(0, &String::from("name")),Ok(_)));
    assert_eq!(result_set.line_count(), 1);
    assert_eq!(result_set.column_count(), 1);
}

#[test]
pub fn test_check_signed_smallint_line_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_signed_smallint(8i16);
    tuples.push(tuple);

    let result_set = ResultSet::new_select(columns, tuples);

    assert!(matches!(result_set.get_signed_smallint(0, &String::from("name")),Ok(_)));
    assert_eq!(result_set.line_count(), 1);
    assert_eq!(result_set.column_count(), 1);
}

#[test]
pub fn test_check_signed_int_line_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_signed_int(8i32);
    tuples.push(tuple);

    let result_set = ResultSet::new_select(columns, tuples);

    assert!(matches!(result_set.get_signed_int(0, &String::from("name")),Ok(_)));
    assert_eq!(result_set.line_count(), 1);
    assert_eq!(result_set.column_count(), 1);
}

#[test]
pub fn test_check_signed_bigint_line_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_signed_bigint(8i64);
    tuples.push(tuple);

    let result_set = ResultSet::new_select(columns, tuples);

    assert!(matches!(result_set.get_signed_bigint(0, &String::from("name")),Ok(_)));
    assert_eq!(result_set.line_count(), 1);
    assert_eq!(result_set.column_count(), 1);
}

#[test]
pub fn test_check_signed_bigint_and_string_line_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("id"), ColumnType::UnsignedBigint));
    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(8u64);
    tuple.push_string(&String::from("database1"));
    tuples.push(tuple);

    let result_set = ResultSet::new_select(columns, tuples);

    assert!(matches!(result_set.get_unsigned_bigint(0, &String::from("id")),Ok(_)));
    assert!(matches!(result_set.get_string(0, &String::from("name")),Ok(_)));
    assert_eq!(result_set.line_count(), 1);
    assert_eq!(result_set.column_count(), 2);
}

#[test]
pub fn test_check_signed_bigint_with_string_line_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("id"), ColumnType::UnsignedBigint));

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(8u64);
    tuples.push(tuple);

    let result_set = ResultSet::new_select(columns, tuples);

    assert!(matches!(result_set.get_string(0, &String::from("id")),Err(ParserError::WrongFormat)));
    assert_eq!(result_set.line_count(), 1);
    assert_eq!(result_set.column_count(), 1);
}

#[test]
pub fn test_check_result_on_result_set_with_two_lines() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_string(&String::from("database2"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_string(&String::from("database3"));
    tuples.push(tuple1);

    let result_set = ResultSet::new_select(columns, tuples);

    assert!(
        matches!(
            result_set.get_string(0, &String::from("name")),
            Ok(_)
        )
    );
    assert!(
        matches!(
            result_set.get_string(1, &String::from("name")),
            Ok(_)
        )
    );
    assert_eq!(result_set.line_count(), 2);
    assert_eq!(result_set.column_count(), 1);
}
