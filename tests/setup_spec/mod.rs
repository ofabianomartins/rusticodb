use std::path::Path;

use rusticodb::config::Config;
use rusticodb::storage::machine::Machine;
use rusticodb::setup::setup_system;

use crate::test_utils::destroy_tmp_test_folder;
use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_system_database_folder() {
    let mut machine = Machine::new();

    setup_system(&mut machine);

    let metadata_foldername = format!("{}", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_create_database_rusticodb_folder() {
    create_tmp_test_folder();

    let mut machine = Machine::new();

    setup_system(&mut machine);

    let metadata_foldername = format!("{}/rusticodb/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_create_table_tables_data_file() {
    create_tmp_test_folder();

    let mut machine = Machine::new();

    setup_system(&mut machine);

    let table_filename = format!("{}/rusticodb/tables.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_create_table_databases_data_file() {
    create_tmp_test_folder();

    let mut machine = Machine::new();

    setup_system(&mut machine);

    let table_filename = format!("{}/rusticodb/databases.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_create_table_columns_data_file() {
    create_tmp_test_folder();

    let mut machine = Machine::new();

    setup_system(&mut machine);

    let table_filename = format!("{}/rusticodb/columns.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    destroy_tmp_test_folder();
}
