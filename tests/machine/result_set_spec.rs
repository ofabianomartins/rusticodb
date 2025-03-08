use rusticodb::machine::ResultSet;
use rusticodb::machine::column::{Column, ColumnType};
use rusticodb::machine::raw_val::RawVal;
use rusticodb::machine::Expression;
use rusticodb::machine::Expression2Type;

use rusticodb::storage::Tuple;

use rusticodb::utils::ExecutionError;

#[test]
pub fn test_check_string_line_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(
        Column::new(
            String::from("rusticodb"),
            String::from("databases"),
            String::from("name"),
            ColumnType::Varchar,
            false,
            false,
            false,
            String::from("")
        )
    );

    let mut tuple = Tuple::new();
    tuple.push_varchar(&String::from("database1"));
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

    columns.push(
        Column::new(
            String::from("rusticodb"), 
            String::from("databases"), 
            String::from("name"), 
            ColumnType::Varchar, 
            false, 
            false, 
            false,
            String::from("")
        )
    );

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

    columns.push(
        Column::new(
            String::from("rusticodb"),
            String::from("databases"),
            String::from("name"),
            ColumnType::Varchar,
            false,
            false,
            false,
            String::from("")
        )
    );

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

    columns.push(
        Column::new(
            String::from("rusticodb"),
            String::from("databases"), 
            String::from("name"),
            ColumnType::Varchar,
            false,
            false,
            false,
            String::from("")
        )
    );

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

    columns.push(
        Column::new(
            String::from("rusticodb"),
            String::from("databases"),
            String::from("name"),
            ColumnType::Varchar,
            false,
            false,
            false,
            String::from("")
        )
    );

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

    columns.push(
        Column::new(
            String::from("rusticodb"),
            String::from("databases"),
            String::from("name"),
            ColumnType::Varchar,
            false,
            false,
            false,
            String::from("")
        )
    );

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

    columns.push(
        Column::new(
            String::from("rusticodb"),
            String::from("databases"),
            String::from("name"),
            ColumnType::Varchar,
            false,
            false,
            false,
            String::from("")
        )
    );

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

    columns.push(
        Column::new(
            String::from("rusticodb"),
            String::from("databases"),
            String::from("name"),
            ColumnType::Varchar,
            false,
            false,
            false,
            String::from("")
        )
    );

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

    columns.push(
        Column::new(
            String::from("rusticodb"),
            String::from("databases"),
            String::from("name"),
            ColumnType::Varchar,
            false,
            false,
            false,
            String::from("")
        )
    );

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

    columns.push(
        Column::new(
            String::from("rusticodb"),
            String::from("databases"),
            String::from("name"),
            ColumnType::Varchar,
            false,
            false,
            false,
            String::from("")
        )
    );

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

    columns.push(
        Column::new(
            String::from("rusticodb"),
            String::from("databases"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            false,
            false,
            false,
            String::from("")
        )
    );
    columns.push(
        Column::new(
            String::from("rusticodb"),
            String::from("databases"),
            String::from("name"),
            ColumnType::Varchar,
            false,
            false,
            false,
            String::from("")
        )
    );

    let mut tuple = Tuple::new();
    tuple.push_unsigned_bigint(8u64);
    tuple.push_varchar(&String::from("database1"));
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

    columns.push(
        Column::new(
            String::from("rusticodb"),
            String::from("databases"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            false,
            false,
            false,
            String::from("")
        )
    );

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

    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));

    let mut tuple = Tuple::new();
    tuple.push_varchar(&String::from("database2"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_varchar(&String::from("database3"));
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

    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("last_name"), ColumnType::Varchar, false, false, false, String::from("")));

    let mut tuple = Tuple::new();
    tuple.push_varchar(&String::from("fabiano"));
    tuple.push_varchar(&String::from("martins"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_varchar(&String::from("fabiano"));
    tuple1.push_varchar(&String::from("martins"));
    tuples.push(tuple1);

    let mut projection_columns: Vec<Column> = Vec::new();

    projection_columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));

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

    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("last_name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("country"), ColumnType::Varchar, false, false, false, String::from("")));

    let mut tuple = Tuple::new();
    tuple.push_varchar(&String::from("fabiano"));
    tuple.push_varchar(&String::from("martins"));
    tuple.push_varchar(&String::from("Brazil"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_varchar(&String::from("fabiano"));
    tuple1.push_varchar(&String::from("martins"));
    tuple1.push_varchar(&String::from("Brazil"));
    tuples.push(tuple1);

    let mut projection_columns: Vec<Column> = Vec::new();

    projection_columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));
    projection_columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("last_name"), ColumnType::Varchar, false, false, false, String::from("")));

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
pub fn test_cartesian_product_between_a_empty_and_full_result_sets() {
    let columns: Vec<Column> = Vec::new();
    let tuples: Vec<Tuple> = Vec::new();
    let empty_set = ResultSet::new_select(columns, tuples);

    let mut columns1: Vec<Column> = Vec::new();
    let mut tuples1: Vec<Tuple> = Vec::new();

    columns1.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));

    let mut tuple = Tuple::new();
    tuple.push_varchar(&String::from("database2"));
    tuples1.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_varchar(&String::from("database3"));
    tuples1.push(tuple1);

    let result_set = ResultSet::new_select(columns1, tuples1);
    let new_set = result_set.cartesian_product(&empty_set);

    assert_eq!(new_set.column_count(), 1);
    assert_eq!(new_set.line_count(), 2);
    assert!(
        matches!(
            new_set.get_string(0, &String::from("name")),
            Ok(_)
        )
    );
}

#[test]
pub fn test_cartesian_product_between_two_result_sets() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));

    let mut tuple = Tuple::new();
    tuple.push_varchar(&String::from("database2"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_varchar(&String::from("database3"));
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

    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("last_name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("country"), ColumnType::Varchar, false, false, false, String::from("")));

    let mut tuple = Tuple::new();
    tuple.push_varchar(&String::from("fabiano"));
    tuple.push_varchar(&String::from("martins"));
    tuple.push_varchar(&String::from("Brazil"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_varchar(&String::from("fabiano"));
    tuple1.push_varchar(&String::from("martins"));
    tuple1.push_varchar(&String::from("Brazil"));
    tuples.push(tuple1);

    let result_set = ResultSet::new_select(columns.clone(), tuples);

    let mut columns2: Vec<Column> = Vec::new();
    let mut tuples2: Vec<Tuple> = Vec::new();

    columns2.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns2.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("last_name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns2.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("country"), ColumnType::Varchar, false, false, false, String::from("")));

    let mut tuple = Tuple::new();
    tuple.push_varchar(&String::from("fabiano"));
    tuple.push_varchar(&String::from("martins"));
    tuple.push_varchar(&String::from("Brazil"));
    tuples2.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_varchar(&String::from("fabiano"));
    tuple1.push_varchar(&String::from("martins"));
    tuple1.push_varchar(&String::from("Brazil"));
    tuples2.push(tuple1);

    let result_set2 = ResultSet::new_select(columns, tuples2);

    let new_set_result = result_set.union(&result_set2);

    let new_set = new_set_result.unwrap();

    assert_eq!(new_set.line_count(), 4);
    assert_eq!(new_set.column_count(), 3);
    // assert!(matches!(new_set_result, Ok(_new_set)));
}

#[test]
pub fn test_selection_of_two_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("last_name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("country"), ColumnType::Varchar, false, false, false, String::from("")));

    let mut tuple = Tuple::new();
    tuple.push_varchar(&String::from("fabiano"));
    tuple.push_varchar(&String::from("martins"));
    tuple.push_varchar(&String::from("Brazil"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_varchar(&String::from("Renato"));
    tuple1.push_varchar(&String::from("martins"));
    tuple1.push_varchar(&String::from("Brazil"));
    tuples.push(tuple1);

    let mut tuple2 = Tuple::new();
    tuple2.push_varchar(&String::from("Renato"));
    tuple2.push_varchar(&String::from("martins"));
    tuple2.push_varchar(&String::from("United States"));
    tuples.push(tuple2);

    let result_set = ResultSet::new_select(columns.clone(), tuples);

    let condition = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("name"))),
        Box::new(Expression::Const(RawVal::Str(String::from("fabiano"))))
    );

    let new_set_result = result_set.selection(condition);

    let new_set = new_set_result.unwrap();

    assert_eq!(new_set.line_count(), 1);
    assert_eq!(new_set.column_count(), 3);
}

#[test]
pub fn test_limit_to_two_of_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("last_name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("country"), ColumnType::Varchar, false, false, false, String::from("")));

    let mut tuple = Tuple::new();
    tuple.push_varchar(&String::from("fabiano"));
    tuple.push_varchar(&String::from("martins"));
    tuple.push_varchar(&String::from("Brazil"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_varchar(&String::from("Renato"));
    tuple1.push_varchar(&String::from("martins"));
    tuple1.push_varchar(&String::from("Brazil"));
    tuples.push(tuple1);

    let mut tuple2 = Tuple::new();
    tuple2.push_varchar(&String::from("Renato"));
    tuple2.push_varchar(&String::from("martins"));
    tuple2.push_varchar(&String::from("United States"));
    tuples.push(tuple2);

    let result_set = ResultSet::new_select(columns.clone(), tuples);

    let new_set = result_set.limit(1usize);

    assert_eq!(new_set.line_count(), 1);
    assert_eq!(new_set.column_count(), 3);
}

#[test]
pub fn test_limit_bigger_than_of_result_set_size() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("last_name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("country"), ColumnType::Varchar, false, false, false, String::from("")));

    let mut tuple = Tuple::new();
    tuple.push_varchar(&String::from("fabiano"));
    tuple.push_varchar(&String::from("martins"));
    tuple.push_varchar(&String::from("Brazil"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_varchar(&String::from("Renato"));
    tuple1.push_varchar(&String::from("martins"));
    tuple1.push_varchar(&String::from("Brazil"));
    tuples.push(tuple1);

    let mut tuple2 = Tuple::new();
    tuple2.push_varchar(&String::from("Renato"));
    tuple2.push_varchar(&String::from("martins"));
    tuple2.push_varchar(&String::from("United States"));
    tuples.push(tuple2);

    let result_set = ResultSet::new_select(columns, tuples);

    let new_set = result_set.limit(100usize);

    assert_eq!(new_set.line_count(), 3);
    assert_eq!(new_set.column_count(), 3);
}

#[test]
pub fn test_offset_to_two_of_result_set_size() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("last_name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("country"), ColumnType::Varchar, false, false, false, String::from("")));

    let mut tuple = Tuple::new();
    tuple.push_varchar(&String::from("fabiano"));
    tuple.push_varchar(&String::from("martins"));
    tuple.push_varchar(&String::from("Brazil"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_varchar(&String::from("Renato"));
    tuple1.push_varchar(&String::from("martins"));
    tuple1.push_varchar(&String::from("Brazil"));
    tuples.push(tuple1);

    let mut tuple2 = Tuple::new();
    tuple2.push_varchar(&String::from("Renato"));
    tuple2.push_varchar(&String::from("martins"));
    tuple2.push_varchar(&String::from("United States"));
    tuples.push(tuple2);

    let result_set = ResultSet::new_select(columns.clone(), tuples);

    let new_set = result_set.offset(1usize);

    assert_eq!(new_set.line_count(), 2);
    assert_eq!(new_set.column_count(), 3);

    assert_eq!(
      new_set.get_string(0, &String::from("name")).unwrap(),
      String::from("Renato")
    );
}

#[test]
pub fn test_offset_bigger_of_result_set_size() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("last_name"), ColumnType::Varchar, false, false, false, String::from("")));
    columns.push(Column::new(String::from("rusticodb"), String::from("databases"), String::from("country"), ColumnType::Varchar, false, false, false, String::from("")));

    let mut tuple = Tuple::new();
    tuple.push_varchar(&String::from("fabiano"));
    tuple.push_varchar(&String::from("martins"));
    tuple.push_varchar(&String::from("Brazil"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_varchar(&String::from("Renato"));
    tuple1.push_varchar(&String::from("martins"));
    tuple1.push_varchar(&String::from("Brazil"));
    tuples.push(tuple1);

    let mut tuple2 = Tuple::new();
    tuple2.push_varchar(&String::from("Renato"));
    tuple2.push_varchar(&String::from("martins"));
    tuple2.push_varchar(&String::from("United States"));
    tuples.push(tuple2);

    let result_set = ResultSet::new_select(columns, tuples);

    let new_set = result_set.offset(100usize);

    assert_eq!(new_set.line_count(), 0);
    assert_eq!(new_set.column_count(), 3);
}

