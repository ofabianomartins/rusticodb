use rusticodb::machine::Machine;
use rusticodb::parser::sql_executor::SqlExecutor;
use rusticodb::setup::setup_system;
use rusticodb::storage::pager::Pager;
use rusticodb::utils::ExecutionError;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_in_two_columns_varchar() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(name1 VARCHAR, name2 VARCHAR)");
    let result_set = sql_executor.parse_command("INSERT table1 VALUES (\"fabiano\", \"martins\")");


    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = sql_executor.parse_command("SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));
    assert_eq!(result_set_select.as_ref().unwrap()[0].tuples.len(), 1);
    assert_eq!(result_set_select.unwrap()[0].column_count(), 2);
}

#[test]
pub fn test_in_two_columns_varchar_and_with_primary_key() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 VARCHAR, name2 VARCHAR)");
    let result_set = sql_executor.parse_command("INSERT table1(name1, name2) VALUES (\"fabiano\", \"martins\")");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = sql_executor.parse_command("SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 1);
    assert_eq!(rs[0].column_count(), 3);

    assert_eq!(
        rs.get(0).unwrap().get_unsigned_bigint(0, &String::from("id")).unwrap(),
        1u64
    );

    assert_eq!(
        rs.get(0).unwrap().get_string(0, &String::from("name1")).unwrap(),
        String::from("fabiano")
    );

    assert_eq!(
        rs.get(0).unwrap().get_string(0, &String::from("name2")).unwrap(),
        String::from("martins")
    );
}

#[test]
pub fn test_in_two_rows_with_null_value() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 VARCHAR NOT NULL, name2 VARCHAR)");
    let result_set = sql_executor.parse_command("INSERT table1(name1, name2) VALUES (NULL, \"martins\")");

    assert!(matches!(result_set, Err(ExecutionError::ColumnCantBeNull(_result_set, _, _))));

    let result_set_select = sql_executor.parse_command("SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 0);
    assert_eq!(rs[0].column_count(), 3);
}

