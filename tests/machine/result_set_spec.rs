use rusticodb::machine::result_set::ResultSet;
use rusticodb::machine::column::{Column, ColumnType};
use rusticodb::utils::execution_error::ExecutionError;
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

    assert!(matches!(result_set.get_string(0, &String::from("id")),Err(ExecutionError::WrongFormat)));
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

#[test]
pub fn test_projection_in_one_column() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));
    columns.push(Column::new_column(String::from("last_name"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_string(&String::from("fabiano"));
    tuple.push_string(&String::from("martins"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_string(&String::from("fabiano"));
    tuple1.push_string(&String::from("martins"));
    tuples.push(tuple1);

    let mut projection_columns: Vec<Column> = Vec::new();

    projection_columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));

    let result_set = ResultSet::new_select(columns, tuples);
    let new_set_result = result_set.projection(projection_columns);

    let new_set = new_set_result.unwrap();

    assert_eq!(new_set.line_count(), 2);
    assert_eq!(new_set.column_count(), 1);
    // assert!(matches!(new_set_result, Ok(_new_set)));
}

#[test]
pub fn test_projection_in_two_columns() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));
    columns.push(Column::new_column(String::from("last_name"), ColumnType::Varchar));
    columns.push(Column::new_column(String::from("country"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_string(&String::from("fabiano"));
    tuple.push_string(&String::from("martins"));
    tuple.push_string(&String::from("Brazil"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_string(&String::from("fabiano"));
    tuple1.push_string(&String::from("martins"));
    tuple1.push_string(&String::from("Brazil"));
    tuples.push(tuple1);

    let mut projection_columns: Vec<Column> = Vec::new();

    projection_columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));
    projection_columns.push(Column::new_column(String::from("last_name"), ColumnType::Varchar));

    let result_set = ResultSet::new_select(columns, tuples);
    let new_set_result = result_set.projection(projection_columns);

    let new_set = new_set_result.unwrap();

    assert_eq!(new_set.line_count(), 2);
    assert_eq!(new_set.column_count(), 2);
    assert!(
        matches!(
            new_set.get_string(0, &String::from("name")),
            Ok(_)
        )
    );
    assert!(
        matches!(
            new_set.get_string(0, &String::from("last_name")),
            Ok(_)
        )
    );
    assert!(
        matches!(
            new_set.get_string(0, &String::from("country")),
            Err(ExecutionError::ColumnNotExists(_))
        )
    );
    // assert!(matches!(new_set_result, Ok(_new_set)));
}

#[test]
pub fn test_cartesian_product_between_two_result_sets() {
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
    let new_set = result_set.cartesian_product(&result_set);

    assert!(
        matches!(
            new_set.get_string(0, &String::from("name")),
            Ok(_)
        )
    );
    assert!(
        matches!(
            new_set.get_string(1, &String::from("name")),
            Ok(_)
        )
    );
    assert_eq!(new_set.line_count(), 4);
    assert_eq!(new_set.column_count(), 2);
}

#[test]
pub fn test_union_of_two_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));
    columns.push(Column::new_column(String::from("last_name"), ColumnType::Varchar));
    columns.push(Column::new_column(String::from("country"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_string(&String::from("fabiano"));
    tuple.push_string(&String::from("martins"));
    tuple.push_string(&String::from("Brazil"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_string(&String::from("fabiano"));
    tuple1.push_string(&String::from("martins"));
    tuple1.push_string(&String::from("Brazil"));
    tuples.push(tuple1);


    let result_set = ResultSet::new_select(columns.clone(), tuples);

    let mut columns2: Vec<Column> = Vec::new();
    let mut tuples2: Vec<Tuple> = Vec::new();

    columns2.push(Column::new_column(String::from("name"), ColumnType::Varchar));
    columns2.push(Column::new_column(String::from("last_name"), ColumnType::Varchar));
    columns2.push(Column::new_column(String::from("country"), ColumnType::Varchar));

    let mut tuple = Tuple::new();
    tuple.push_string(&String::from("fabiano"));
    tuple.push_string(&String::from("martins"));
    tuple.push_string(&String::from("Brazil"));
    tuples2.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_string(&String::from("fabiano"));
    tuple1.push_string(&String::from("martins"));
    tuple1.push_string(&String::from("Brazil"));
    tuples2.push(tuple1);


    let result_set2 = ResultSet::new_select(columns, tuples2);


    let new_set_result = result_set.union(&result_set2);

    let new_set = new_set_result.unwrap();

    assert_eq!(new_set.line_count(), 4);
    assert_eq!(new_set.column_count(), 3);
    // assert!(matches!(new_set_result, Ok(_new_set)));
}
