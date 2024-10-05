use std::path::Path;

use rusticodb::config::Config;
use rusticodb::machine::Machine;
use rusticodb::storage::pager::Pager;
use rusticodb::storage::os_interface::BLOCK_SIZE;

use crate::test_utils::create_tmp_test_folder;
use crate::test_utils::destroy_tmp_test_folder;
use crate::test_utils::read_from_file;

#[test]
pub fn test_write_data_metadata_file() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");

    let data = [2u8; BLOCK_SIZE];

    let mut machine = Machine::new();
    let mut pager = Pager::new();

    create_tmp_test_folder();

    machine.create_database(&database1);
    machine.create_table(&database1, &table1);
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

    let mut machine = Machine::new();
    let mut pager = Pager::new();

    create_tmp_test_folder();

    machine.create_database(&database1);
    machine.create_table(&database1, &table1);
    pager.write_data(&database1, &table1, 0u64, &data);

    let actual_content = pager.read_data(&database1, &table1, 0u64);
    assert_eq!(actual_content, data, "File content does not match expected content");

    destroy_tmp_test_folder();
}
