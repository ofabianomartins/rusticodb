use rusticodb::machine::result_set::ResultSet;
use rusticodb::machine::column::Column;
use rusticodb::storage::tuple::Tuple;

#[test]
pub fn test_check_result_on_result_set() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name")));

    let mut tuple = Tuple::new();
    tuple.push_string(&String::from("database1"));
    tuples.push(tuple);

    let mut result_set = ResultSet::new_select(columns, tuples);

    assert!(
        matches!(
            result_set.get_string(0, &String::from("name")),
            Ok(_)
        )
    );
    assert_eq!(result_set.line_count(), 1);
    assert_eq!(result_set.column_count(), 1);
}

#[test]
pub fn test_check_result_on_result_set_with_two_lines() {
    let mut columns: Vec<Column> = Vec::new();
    let mut tuples: Vec<Tuple> = Vec::new();

    columns.push(Column::new_column(String::from("name")));

    let mut tuple = Tuple::new();
    tuple.push_string(&String::from("database2"));
    tuples.push(tuple);

    let mut tuple1 = Tuple::new();
    tuple1.push_string(&String::from("database3"));
    tuples.push(tuple1);

    let mut result_set = ResultSet::new_select(columns, tuples);


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
