use uuid::Uuid;
use std::path::Path;

use std::fs::remove_dir_all;

use rusticodb::execute::execute;
use rusticodb::config::Config;

#[test]
fn test_execute_create_table_users() {
    let id = Uuid::new_v4();

    let config: Config = Config { base_path: String::from("/tmp/rusticodb/main") };

    let users_folder_path_str = format!("{0}/tables/users", config.base_path.clone());
    let users_folder_path: &str = &users_folder_path_str[..];

    let users_metadata_folder_path_str = format!("{users_folder_path}/metadata.json");
    let users_metadata_folder_path: &str = &users_metadata_folder_path_str[..];

    let file_path_str = format!("/tmp/database_{0}.database", id);
    let file_path: &str = &file_path_str[..];

    let data_file_path_str = format!("{0}/data.json", config.base_path.clone());
    let data_file_path: &str = &data_file_path_str[..];

    let sql = "CREATE TABLE users(id INTEGER, name VARCHAR)";

    let _ = remove_dir_all(config.base_path.clone());

    execute(&config, file_path, sql);

    assert!(Path::new(file_path).exists());
    assert!(Path::new(&config.base_path).exists());
    assert!(Path::new(data_file_path).exists());
    assert!(Path::new(users_folder_path).exists());
    assert!(Path::new(users_metadata_folder_path).exists());
}

#[test]
fn test_execute_create_table_messages() {
    let id = Uuid::new_v4();

    let config: Config = Config { base_path: String::from("/tmp/rusticodb/main") };

    let users_folder_path_str = format!("{0}/tables/messages", config.base_path.clone());
    let users_folder_path: &str = &users_folder_path_str[..];

    let users_metadata_folder_path_str = format!("{users_folder_path}/metadata.json");
    let users_metadata_folder_path: &str = &users_metadata_folder_path_str[..];

    let file_path_str = format!("/tmp/database_{0}.database", id);
    let file_path: &str = &file_path_str[..];

    let data_file_path_str = format!("{0}/data.json", config.base_path.clone());
    let data_file_path: &str = &data_file_path_str[..];

    let sql = "CREATE TABLE messages(id INTEGER, content VARCHAR)";

    let _ = remove_dir_all(config.base_path.clone());

    execute(&config, file_path, sql);

    assert!(Path::new(file_path).exists());
    assert!(Path::new(&config.base_path).exists());
    assert!(Path::new(data_file_path).exists());
    assert!(Path::new(users_folder_path).exists());
    assert!(Path::new(users_metadata_folder_path).exists());
}
