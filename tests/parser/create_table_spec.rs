use std::path::Path;

use rusticodb::config::Config;
use rusticodb::machine::context::Context;
use rusticodb::machine::machine::Machine;
use rusticodb::utils::execution_error::ExecutionError;
use rusticodb::parser::sql_executor::SqlExecutor;
use rusticodb::setup::setup_system;
use rusticodb::storage::pager::Pager;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_create_table_metadata_file() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert!(sql_executor.machine.context.check_table_exists(&database_name, &table_name));
    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_create_table_without_set_database() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let error_parse = sql_executor.parse_command("CREATE TABLE table1");

    assert!(
        matches!(
            error_parse,
            Err(ExecutionError::DatabaseNotSetted)
        )
    );

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert_eq!(sql_executor.machine.context.check_table_exists(&database_name, &table_name), false);
    assert!(matches!(sql_executor.machine.context.actual_database, None));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&table_filename).exists(), false);
}

#[test]
pub fn test_create_table_that_already_exists() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1");
    let error_parse = sql_executor.parse_command("CREATE TABLE table1");

    assert!(matches!(error_parse, Err(ExecutionError::DatabaseExists(_result_set))));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert!(sql_executor.machine.context.check_table_exists(&database_name, &table_name));
    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_create_table_with_if_not_exists() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1");
    let result_set = sql_executor.parse_command("CREATE TABLE IF NOT EXISTS table1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert!(sql_executor.machine.context.check_table_exists(&database_name, &table_name));
    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_create_table_with_two_columns() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(name1 VARCHAR, name2 VARCHAR)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    let column_name1 = String::from("name1");
    let column_name2 = String::from("name2");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert!(sql_executor.machine.context.check_table_exists(&database_name, &table_name));
    assert!(sql_executor.machine.context.check_column_exists(&database_name, &table_name, &column_name1));
    assert!(sql_executor.machine.context.check_column_exists(&database_name, &table_name, &column_name2));
    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}
