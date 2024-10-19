use std::path::Path;

use rusticodb::config::Config;
use rusticodb::machine::machine::Machine;
use rusticodb::storage::cell::Cell;
use rusticodb::storage::cell::CellType;
use rusticodb::storage::tuple::Tuple;
use rusticodb::storage::pager::Pager;
use rusticodb::storage::os_interface::BLOCK_SIZE;

use crate::test_utils::create_tmp_test_folder;
use crate::test_utils::destroy_tmp_test_folder;
use crate::test_utils::read_from_file;

#[test]
pub fn test_if_database_exists_is_true() {
    let database1 = String::from("database1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    machine.create_database(&database1);
    assert!(machine.database_exists(&database1));

    destroy_tmp_test_folder();
}

#[test]
pub fn test_if_database_exists_is_false() {
    let database1 = String::from("database1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    assert_eq!(machine.database_exists(&database1), false);

    destroy_tmp_test_folder();
}

#[test]
pub fn test_if_table_exists_is_true() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    machine.create_database(&database1);
    machine.create_table(&database1, &table1);
    assert!(machine.table_exists(&database1, &table1));

    destroy_tmp_test_folder();
}

#[test]
pub fn test_if_table_exists_is_false() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    machine.create_database(&database1);
    assert_eq!(machine.table_exists(&database1, &table1), false);

    destroy_tmp_test_folder();
}

#[test]
pub fn test_create_database_metadata_file_database1() {
    let database1 = String::from("database1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    machine.create_database(&database1);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_create_table_metadata_file() {
    let database1 = String::from("database1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    machine.create_database(&database1);
    machine.create_table(&database1, &String::from("table1"));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    destroy_tmp_test_folder();
}
