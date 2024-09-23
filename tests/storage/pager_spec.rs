use std::path::Path;

use rusticodb::storage::pager::Pager;
use rusticodb::storage::config::Config;
use rusticodb::storage::pager::BLOCK_SIZE;

use crate::test_utils::create_tmp_test_folder;
use crate::test_utils::destroy_tmp_test_folder;
use crate::test_utils::read_from_file;

#[test]
pub fn test_create_database_metadata_file_database1() {
    let database1 = String::from("database1");
    let mut pager = Pager::new();

    create_tmp_test_folder();

    pager.create_database(&database1);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_create_table_metadata_file() {
    let database1 = String::from("database1");

    let mut pager = Pager::new();

    create_tmp_test_folder();

    pager.create_database(&database1);
    pager.create_file(&database1, &String::from("table1"));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_write_data_metadata_file() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");

    let data = [2u8; BLOCK_SIZE];

    let mut pager = Pager::new();

    create_tmp_test_folder();

    pager.create_database(&database1);
    pager.create_file(&database1, &table1);
    pager.write_data(&database1, &table1, 0u64, &data);

    let metadata_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&metadata_filename).exists());

    let rows_filename = format!("{}/{}/{}.db", Config::data_folder(), &database1, &table1);
     // Read the content back from the file
    let actual_content = read_from_file(&rows_filename).expect("Failed to read from file");
    assert_eq!(actual_content, data, "File content does not match expected content");

    destroy_tmp_test_folder();
}

#[test]
pub fn test_read_data_metadata_file() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");

    let data = [2u8; BLOCK_SIZE];

    let mut pager = Pager::new();

    create_tmp_test_folder();

    pager.create_database(&database1);
    pager.create_file(&database1, &table1);
    pager.write_data(&database1, &table1, 0u64, &data);

    let actual_content = pager.read_data(&database1, &table1, 0u64);
    assert_eq!(actual_content, data, "File content does not match expected content");

    destroy_tmp_test_folder();
}
