use std::path::Path;

use rusticodb::config::Config;

use rusticodb::machine::Machine;
use rusticodb::machine::Table;
use rusticodb::machine::create_database;
use rusticodb::machine::create_table;
use rusticodb::machine::check_table_exists;
use rusticodb::machine::check_database_exists;

use rusticodb::storage::pager::Pager;

use rusticodb::setup::setup_system;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_if_database_exists_is_true() {
    let database1 = String::from("database1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = create_database(&mut machine, database1.clone(), false);
    assert_eq!(check_database_exists(&mut machine, &database1), true);
}

#[test]
pub fn test_if_database_exists_is_false() {
    let database1 = String::from("database1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    assert_eq!(check_database_exists(&mut machine, &database1), false);
}

#[test]
pub fn test_if_table_exists_is_true() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = create_database(&mut machine, database1.clone(), false);
    let table = Table::new(database1.clone(), table1.clone());
    let _ = create_table(&mut machine, &table, Vec::new());
    assert_eq!(check_table_exists(&mut machine, &table), true);
}

#[test]
pub fn test_if_table_exists_is_false() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = create_database(&mut machine, database1.clone(), false);
    let table = Table::new(database1.clone(), table1.clone());
    assert_eq!(check_table_exists(&mut machine, &table), false);
}

#[test]
pub fn test_create_database_metadata_file_database1() {
    let database1 = String::from("database1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = create_database(&mut machine, database1.clone(), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_table_metadata_file() {
    let database1 = String::from("database1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let table = Table::new(database1.clone(), String::from("table1"));
    let _ = create_database(&mut machine, database1.clone(), false);
    let _ = create_table(&mut machine, &table, Vec::new());

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}
