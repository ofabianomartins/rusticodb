use std::path::Path;

use rusticodb::config::Config;
use rusticodb::machine::context::Context;
use rusticodb::machine::machine::Machine;
use rusticodb::machine::result_set::ExecutionError;
use rusticodb::parser::sql_executor::SqlExecutor;
use rusticodb::setup::setup_system;
use rusticodb::storage::pager::Pager;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_create_database_metadata_file_database1() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let result_set = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(machine.context.check_database_exists(&database_name));
    assert_eq!(matches!(machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_database_with_if_not_exists() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");
    let result_set = sql_executor.parse_command(&mut machine, "CREATE DATABASE IF NOT EXISTS database1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(machine.context.check_database_exists(&database_name));
    assert_eq!(matches!(machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_two_databases() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let result_set = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1; CREATE DATABASE database2");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(machine.context.check_database_exists(&database_name));

    let database_name = String::from("database2");
    assert!(machine.context.check_database_exists(&database_name));

    assert_eq!(matches!(machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_database_with_if_not_exists_in_wrong_order() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");
    let error_parse = sql_executor.parse_command(&mut machine, "CREATE IF NOT EXISTS DATABASE database1");

    assert!(
        matches!(
            error_parse,
            Err(ExecutionError::ParserError(_))
        )
    );

    let database_name = String::from("database1");
    assert!(machine.context.check_database_exists(&database_name));
    assert_eq!(matches!(machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_database_that_already_exists() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");
    let error_parse = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");

    assert!(matches!(error_parse, Err(ExecutionError::DatabaseExists(_result_set))));

    let database_name = String::from("database1");
    assert!(machine.context.check_database_exists(&database_name));
    assert_eq!(matches!(machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_use_database_that_not_exists() {
    let database_name = String::from("database1");
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let error_parse = sql_executor.parse_command(&mut machine, "USE database1");

    assert!(
        matches!(
            error_parse, 
            Err(ExecutionError::DatabaseNotExists(_))
        )
    );

    assert!(matches!(machine.context.actual_database, None));
    assert_eq!(machine.context.check_database_exists(&database_name), false);
}

#[test]
pub fn test_use_database_set_in_context() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");
    let result_set = sql_executor.parse_command(&mut machine, "USE database1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(machine.context.check_database_exists(&database_name));
    assert!(
        matches!(
            machine.context.actual_database, 
            Some(_database_name)
        )
    );

    let metadata_foldername = format!("{}/{}", Config::data_folder(), database_name);
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_table_metadata_file() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = sql_executor.parse_command(&mut machine, "USE database1");
    let _ = sql_executor.parse_command(&mut machine, "CREATE TABLE table1");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(machine.context.check_database_exists(&database_name));
    assert!(machine.context.check_table_exists(&database_name, &table_name));
    assert!(matches!(machine.context.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_create_table_without_set_database() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");
    let error_parse = sql_executor.parse_command(&mut machine, "CREATE TABLE table1");

    assert!(
        matches!(
            error_parse,
            Err(ExecutionError::DatabaseNotSetted)
        )
    );

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(machine.context.check_database_exists(&database_name));
    assert_eq!(machine.context.check_table_exists(&database_name, &table_name), false);
    assert!(matches!(machine.context.actual_database, None));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&table_filename).exists(), false);
}

#[test]
pub fn test_create_table_that_already_exists() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = sql_executor.parse_command(&mut machine, "USE database1");
    let _ = sql_executor.parse_command(&mut machine, "CREATE TABLE table1");
    let error_parse = sql_executor.parse_command(&mut machine, "CREATE TABLE table1");

    assert!(matches!(error_parse, Err(ExecutionError::DatabaseExists(_result_set))));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(machine.context.check_database_exists(&database_name));
    assert!(machine.context.check_table_exists(&database_name, &table_name));
    assert!(matches!(machine.context.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_create_table_with_if_not_exists() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = sql_executor.parse_command(&mut machine, "USE database1");
    let _ = sql_executor.parse_command(&mut machine, "CREATE TABLE table1");
    let result_set = sql_executor.parse_command(&mut machine, "CREATE TABLE IF NOT EXISTS table1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(machine.context.check_database_exists(&database_name));
    assert!(machine.context.check_table_exists(&database_name, &table_name));
    assert!(matches!(machine.context.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_create_table_with_two_columns() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = sql_executor.parse_command(&mut machine, "USE database1");
    let _ = sql_executor.parse_command(&mut machine, "CREATE TABLE table1(name1 VARCHAR, name2 VARCHAR)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    let column_name1 = String::from("name1");
    let column_name2 = String::from("name2");
    assert!(machine.context.check_database_exists(&database_name));
    assert!(machine.context.check_table_exists(&database_name, &table_name));
    assert!(machine.context.check_column_exists(&database_name, &table_name, &column_name1));
    assert!(machine.context.check_column_exists(&database_name, &table_name, &column_name2));
    assert!(matches!(machine.context.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_select_database_tables() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");
    let use_database = sql_executor.parse_command(&mut machine, "USE rusticodb;");
    let result_set = sql_executor.parse_command(&mut machine, "SELECT name FROM databases");

    assert!(matches!(use_database, Ok(_result_set)));
    // assert!(matches!(result_set, Ok(ref result_sets)));

    assert!(matches!(result_set.unwrap().get(0).unwrap().get_string(0, &String::from("name")), Ok(_database_name)));

    let database_name = String::from("database1");
    assert!(machine.context.check_database_exists(&database_name));

    assert!(matches!(machine.context.actual_database, Some(_database_name)));
}
