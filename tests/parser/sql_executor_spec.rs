use std::path::Path;

use rusticodb::storage::config::Config;
use rusticodb::parser::sql_executor::SqlExecutor;

use crate::test_utils::create_tmp_test_folder;
use crate::test_utils::destroy_tmp_test_folder;

#[test]
pub fn test_create_database_metadata_file_database1() {
    let mut sql_executor = SqlExecutor::new();

    create_tmp_test_folder();

    sql_executor.parse_command("CREATE DATABASE database1");

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_create_table_metadata_file() {
    let mut sql_executor = SqlExecutor::new();

    create_tmp_test_folder();

    sql_executor.parse_command("CREATE DATABASE database1");
    sql_executor.parse_command("USE database1");
    sql_executor.parse_command("CREATE TABLE table1");

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    destroy_tmp_test_folder();
}
