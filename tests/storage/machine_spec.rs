use std::path::Path;

use rusticodb::config::Config;
use rusticodb::storage::machine::Machine;

use crate::test_utils::create_tmp_test_folder;
use crate::test_utils::destroy_tmp_test_folder;

#[test]
pub fn test_create_database_metadata_file_database1() {
    let database1 = String::from("database1");
    let mut machine = Machine::new();

    create_tmp_test_folder();

    machine.create_database(&database1);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_create_table_metadata_file() {
    let database1 = String::from("database1");
    let mut machine = Machine::new();

    create_tmp_test_folder();

    machine.create_database(&database1);
    machine.create_table(&database1, &String::from("table1"));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    destroy_tmp_test_folder();
}
