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
pub fn test_create_database_metadata_file_database1() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let result_set = sql_executor.parse_command("CREATE DATABASE database1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert_eq!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_database_with_if_not_exists() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let result_set = sql_executor.parse_command("CREATE DATABASE IF NOT EXISTS database1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert_eq!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_database_with_if_not_exists_in_wrong_order() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let error_parse = sql_executor.parse_command("CREATE IF NOT EXISTS DATABASE database1");

    assert!(
        matches!(
            error_parse,
            Err(ExecutionError::ParserError(_))
        )
    );

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert_eq!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_two_databases() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let result_set = sql_executor.parse_command("CREATE DATABASE database1; CREATE DATABASE database2");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));

    let database_name = String::from("database2");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));

    assert_eq!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_database_that_already_exists() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let error_parse = sql_executor.parse_command("CREATE DATABASE database1");

    assert!(matches!(error_parse, Err(ExecutionError::DatabaseExists(_result_set))));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert_eq!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}
