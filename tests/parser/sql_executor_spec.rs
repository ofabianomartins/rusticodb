use std::path::Path;

use rusticodb::config::Config;
use rusticodb::machine::context::Context;
use rusticodb::machine::machine::Machine;
use rusticodb::parser::sql_executor::SqlExecutor;
use rusticodb::storage::pager::Pager;

use crate::test_utils::create_tmp_test_folder;
use crate::test_utils::destroy_tmp_test_folder;

#[test]
pub fn test_create_database_metadata_file_database1() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");

    let database_name = String::from("database1");
    assert!(machine.context.check_database_exists(&database_name));
    assert_eq!(matches!(machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_use_database_set_in_context() {
    let mut sql_executor = SqlExecutor::new();
    let context = Context::new();
    let pager = Pager::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = sql_executor.parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = sql_executor.parse_command(&mut machine, "USE database1");

    let database_name = String::from("database1");
    assert!(machine.context.check_database_exists(&database_name));
    assert!(matches!(machine.context.actual_database, Some(_database_name)));

    let metadata_foldername = format!("{}/{}", Config::data_folder(), database_name);
    assert!(Path::new(&metadata_foldername).exists());

    destroy_tmp_test_folder();
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

    destroy_tmp_test_folder();
}
