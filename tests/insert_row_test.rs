mod test_utils;

use uuid::Uuid;
use std::path::Path;

use serde_json::Value;

use rusticodb::connection::Connection;
use rusticodb::database::Database;

use test_utils::*;

#[test]
fn test_insert_one_row_in_messages() {
    let id = Uuid::new_v4();
    let data_file_path = format!("./tmp_tests/{0}.database", id.to_string());
    let sql1 = String::from("CREATE TABLE messages(id INTEGER, content VARCHAR)");
    let sql2 = String::from("INSERT INTO messages VALUES (1, 'message content')");

    create_tmp_test_folder();

    let mut conn: Connection = Connection::load(&data_file_path);

    conn.execute(&sql1);
    conn.execute(&sql2);

    assert!(Path::new(&data_file_path).exists());

    let saved_database = Database::read_database(&data_file_path);

    assert_eq!(saved_database.tables.get(0).unwrap().name.to_string(), "messages");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(0).unwrap().name.to_string(), "id");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(0).unwrap().data_type.to_string(), "INTEGER");

    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(1).unwrap().name.to_string(), "content");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(1).unwrap().data_type.to_string(), "VARCHAR");

    assert_eq!(saved_database.tables.get(0).unwrap().rows.len(), 1);
    let json_row: Value = serde_json::from_str(saved_database.tables.get(0).unwrap().rows.get(0).unwrap()).expect("teste");
    assert_eq!(json_row["id"], "1");
    assert_eq!(json_row["content"], String::from("message content"));

    destroy_tmp_test_folder()
}

#[test]
fn test_insert_two_row_in_messages() {
    let id = Uuid::new_v4();
    let data_file_path = format!("./tmp_tests/{0}.database", id.to_string());
    let sql1 = String::from("CREATE TABLE messages(id INTEGER, content VARCHAR)");
    let sql2 = String::from("INSERT INTO messages VALUES (1, 'message content')");
    let sql3 = String::from("INSERT INTO messages VALUES (2, 'message content2')");

    create_tmp_test_folder();

    let mut conn: Connection = Connection::load(&data_file_path);

    conn.execute(&sql1);
    conn.execute(&sql2);
    conn.execute(&sql3);

    assert!(Path::new(&data_file_path).exists());

    let saved_database = Database::read_database(&data_file_path);

    assert_eq!(saved_database.tables.get(0).unwrap().name.to_string(), "messages");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(0).unwrap().name.to_string(), "id");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(0).unwrap().data_type.to_string(), "INTEGER");

    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(1).unwrap().name.to_string(), "content");
    assert_eq!(saved_database.tables.get(0).unwrap().columns.get(1).unwrap().data_type.to_string(), "VARCHAR");

    assert_eq!(saved_database.tables.get(0).unwrap().rows.len(), 2);
    let json_row: Value = serde_json::from_str(saved_database.tables.get(0).unwrap().rows.get(0).unwrap()).expect("teste");
    assert_eq!(json_row["id"], "1");
    assert_eq!(json_row["content"], String::from("message content"));

    destroy_tmp_test_folder()
}
