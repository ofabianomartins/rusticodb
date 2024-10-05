use std::path::Path;

use rusticodb::config::Config;
use rusticodb::machine::context::Context;
use rusticodb::parser::sql_executor::SqlExecutor;

use crate::test_utils::create_tmp_test_folder;
use crate::test_utils::destroy_tmp_test_folder;

#[test]
pub fn test_create_database_metadata_file_database1() {
    let mut sql_executor = SqlExecutor::new();
    let mut context = Context::new();

    create_tmp_test_folder();

    sql_executor.parse_command(&mut context, "CREATE DATABASE database1");

    assert!(context.check_database_exists(&String::from("database1")));

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_use_database_set_in_context() {
    let mut sql_executor = SqlExecutor::new();
    let mut context = Context::new();

    create_tmp_test_folder();

    sql_executor.parse_command(&mut context, "CREATE DATABASE database1");
    sql_executor.parse_command(&mut context, "USE database1");

    let database_name = String::from("database1");
    assert!(context.check_database_exists(&database_name));
    assert!(matches!(context.actual_database, Some(datbase_name)));

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_create_table_metadata_file() {
    let mut sql_executor = SqlExecutor::new();
    let mut context = Context::new();

    create_tmp_test_folder();

    sql_executor.parse_command(&mut context, "CREATE DATABASE database1");
    sql_executor.parse_command(&mut context, "USE database1");
    sql_executor.parse_command(&mut context, "CREATE TABLE table1");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(context.check_database_exists(&database_name));
    assert!(context.check_table_exists(&database_name, &table_name));
    assert!(matches!(context.actual_database, Some(database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    destroy_tmp_test_folder();
}
