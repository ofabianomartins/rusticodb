use std::path::Path;

use rusticodb::config::Config;
use rusticodb::machine::machine::Machine;
use rusticodb::machine::table::Table;
use rusticodb::utils::execution_error::ExecutionError;
use rusticodb::parser::sql_executor::SqlExecutor;
use rusticodb::setup::setup_system;
use rusticodb::storage::pager::Pager;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_drop_table_metadata_file_table1() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1");

    let metadata_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&metadata_filename).exists(), true);

    let drop_table = sql_executor.parse_command("DROP TABLE table1");

    assert!(matches!(drop_table, Ok(_result_set)));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    let table = Table::new(database_name, table_name);
    assert_eq!(sql_executor.machine.check_table_exists(&table), false);
    assert!(matches!(sql_executor.machine.actual_database, Some(_database_name)));

    assert_eq!(Path::new(&metadata_filename).exists(), false);
}

#[test]
pub fn test_drop_table_metadata_that_not_exists() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");

    let metadata_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&metadata_filename).exists(), false);

    let drop_table = sql_executor.parse_command("DROP TABLE table1");

    assert!(matches!(drop_table, Err(ExecutionError::TableNotExists(_result_set))));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    let table = Table::new(database_name, table_name);
    assert_eq!(sql_executor.machine.check_table_exists(&table), false);
    assert!(matches!(sql_executor.machine.actual_database, Some(_database_name)));

    assert_eq!(Path::new(&metadata_filename).exists(), false);
}

#[test]
pub fn test_drop_table_if_exists() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");

    let metadata_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&metadata_filename).exists(), false);

    let drop_table = sql_executor.parse_command("DROP TABLE IF EXISTS table1");

    assert!(matches!(drop_table, Ok(_result_set)));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    let table = Table::new(database_name, table_name);
    assert_eq!(sql_executor.machine.check_table_exists(&table), false);
    assert!(matches!(sql_executor.machine.actual_database, Some(_database_name)));

    assert_eq!(Path::new(&metadata_filename).exists(), false);
}

