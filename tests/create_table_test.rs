mod test_utils;

use uuid::Uuid;
use std::path::Path;

use rusticodb::execute::execute;

use test_utils::*;

#[test]
fn test_execute_create_table_users() {
    let id = Uuid::new_v4();
    let data_file_path = format!("./tmp_tests/{0}.database", id.to_string());
    let sql = String::from("CREATE TABLE users(id INTEGER, name VARCHAR)");

    create_tmp_test_folder();

    execute(&data_file_path, &sql);

    assert!(Path::new(&data_file_path).exists());

    destroy_tmp_test_folder();
}

#[test]
fn test_execute_create_table_messages() {
    let id = Uuid::new_v4();
    let data_file_path = format!("./tmp_tests/{0}.database", id.to_string());
    let sql = String::from("CREATE TABLE messages(id INTEGER, content VARCHAR)");

    create_tmp_test_folder();

    execute(&data_file_path, &sql);

    assert!(Path::new(&data_file_path).exists());

    destroy_tmp_test_folder();
}
