mod test_utils;

use uuid::Uuid;
use std::path::Path;

use rusticodb::connection::Connection;
use rusticodb::database::Database;

use test_utils::*;

#[test]
fn test_execute_create_table_users() {
    let id = Uuid::new_v4();
    let data_file_path = format!("./tmp_tests/{0}.database", id.to_string());
    let sql = String::from("CREATE TABLE users(id INTEGER, name VARCHAR)");

    create_tmp_test_folder();

    let mut conn: Connection = Connection::load(&data_file_path);

    conn.execute(&sql);

    assert!(Path::new(&data_file_path).exists());

    let saved_database = Database::read_database(&data_file_path);

    assert_eq!(saved_database.tables.get(0).unwrap().name.to_string(), "users");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(0).unwrap().name.to_string(), "id");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(0).unwrap().data_type.to_string(), "INTEGER");

    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(1).unwrap().name.to_string(), "name");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(1).unwrap().data_type.to_string(), "VARCHAR");

    destroy_tmp_test_folder()
}

#[test]
fn test_execute_create_table_messages() {
    let id = Uuid::new_v4();
    let data_file_path = format!("./tmp_tests/{0}.database", id.to_string());
    let sql = String::from("CREATE TABLE messages(id INTEGER, content VARCHAR)");

    create_tmp_test_folder();

    let mut conn: Connection = Connection::load(&data_file_path);

    conn.execute(&sql);

    assert!(Path::new(&data_file_path).exists());

    let saved_database = Database::read_database(&data_file_path);

    assert_eq!(saved_database.tables.get(0).unwrap().name.to_string(), "messages");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(0).unwrap().name.to_string(), "id");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(0).unwrap().data_type.to_string(), "INTEGER");

    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(1).unwrap().name.to_string(), "content");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(1).unwrap().data_type.to_string(), "VARCHAR");

    destroy_tmp_test_folder()
}

#[test]
fn test_execute_create_two_tables() {
    let id = Uuid::new_v4();
    let data_file_path = format!("./tmp_tests/{0}.database", id.to_string());
    let sql1 = String::from("CREATE TABLE users(id INTEGER, name VARCHAR)");
    let sql2 = String::from("CREATE TABLE messages(id INTEGER, content VARCHAR)");

    create_tmp_test_folder();

    let mut conn: Connection = Connection::load(&data_file_path);

    conn.execute(&sql1);
    conn.execute(&sql2);

    assert!(Path::new(&data_file_path).exists());

    let saved_database = Database::read_database(&data_file_path);

    assert_eq!(saved_database.tables.get(0).unwrap().name.to_string(), "users");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(0).unwrap().name.to_string(), "id");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(0).unwrap().data_type.to_string(), "INTEGER");

    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(1).unwrap().name.to_string(), "name");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(1).unwrap().data_type.to_string(), "VARCHAR");

    assert_eq!(saved_database.tables.get(1).unwrap().name.to_string(), "messages");
    assert_eq!(saved_database.tables.get(1).unwrap().columns.get(0).unwrap().name.to_string(), "id");
    assert_eq!(saved_database.tables.get(1).unwrap().columns.get(0).unwrap().data_type.to_string(), "INTEGER");

    assert_eq!(saved_database.tables.get(1).unwrap().columns.get(1).unwrap().name.to_string(), "content");
    assert_eq!(saved_database.tables.get(1).unwrap().columns.get(1).unwrap().data_type.to_string(), "VARCHAR");

    destroy_tmp_test_folder()
}

#[test]
fn test_execute_create_tables_message_twice() {
    let id = Uuid::new_v4();
    let data_file_path = format!("./tmp_tests/{0}.database", id.to_string());
    let sql = String::from("CREATE TABLE messages(id INTEGER, content VARCHAR)");

    create_tmp_test_folder();

    let mut conn: Connection = Connection::load(&data_file_path);

    conn.execute(&sql);
    conn.execute(&sql);

    assert!(Path::new(&data_file_path).exists());

    let saved_database = Database::read_database(&data_file_path);

    assert_eq!(saved_database.tables.len(), 1);

    assert_eq!(saved_database.tables.get(0).unwrap().name.to_string(), "messages");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(0).unwrap().name.to_string(), "id");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(0).unwrap().data_type.to_string(), "INTEGER");

    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(1).unwrap().name.to_string(), "content");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(1).unwrap().data_type.to_string(), "VARCHAR");

    destroy_tmp_test_folder()
}
