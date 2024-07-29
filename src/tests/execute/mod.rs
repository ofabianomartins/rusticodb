use uuid::Uuid;
use std::path::Path;

use std::fs::remove_dir_all;

use crate::execute::execute;

#[test]
fn test_execute_create_table() {
    let id = Uuid::new_v4();

    let folder_path = "/tmp/rusticodb/main";

    let users_folder_path_str = format!("/tmp/rusticodb/main/tables/users");
    let users_folder_path: &str = &users_folder_path_str[..];

    let users_metadata_folder_path_str = format!("{users_folder_path}/metadata.json");
    let users_metadata_folder_path: &str = &users_metadata_folder_path_str[..];

    let file_path_str = format!("/tmp/database_{id}.database");
    let file_path: &str = &file_path_str[..];

    let data_file_path_str = format!("{folder_path}/data.json");
    let data_file_path: &str = &data_file_path_str[..];

    let sql = "CREATE TABLE users(id INTEGER, name VARCHAR)";

    let _ = remove_dir_all(folder_path);

    execute(file_path, sql);

    assert!(Path::new(file_path).exists());
    assert!(Path::new(folder_path).exists());
    assert!(Path::new(data_file_path).exists());
    assert!(Path::new(users_folder_path).exists());
    assert!(Path::new(users_metadata_folder_path).exists());
}
