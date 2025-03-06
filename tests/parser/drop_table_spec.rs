use std::path::Path;

use rusticodb::config::Config;

use rusticodb::machine::Machine;
use rusticodb::machine::Table;
use rusticodb::machine::check_table_exists;

use rusticodb::utils::ExecutionError;
use rusticodb::parser::parse_command;
use rusticodb::setup::setup_system;
use rusticodb::storage::Pager;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_drop_table_metadata_file_table1() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1");

    let metadata_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&metadata_filename).exists(), true);

    let drop_table = parse_command(&mut machine, "DROP TABLE table1");

    assert!(matches!(drop_table, Ok(_result_set)));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    let table = Table::new(database_name, table_name);
    assert_eq!(check_table_exists(&mut machine, &table), false);
    assert!(matches!(machine.actual_database, Some(_database_name)));

    assert_eq!(Path::new(&metadata_filename).exists(), false);
}

#[test]
pub fn test_drop_table_metadata_that_not_exists() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");

    let metadata_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&metadata_filename).exists(), false);

    let drop_table = parse_command(&mut machine, "DROP TABLE table1");

    assert!(matches!(drop_table, Err(ExecutionError::TableNotExists(_result_set))));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    let table = Table::new(database_name, table_name);
    assert_eq!(check_table_exists(&mut machine, &table), false);
    assert!(matches!(machine.actual_database, Some(_database_name)));

    assert_eq!(Path::new(&metadata_filename).exists(), false);
}

#[test]
pub fn test_drop_table_if_exists() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");

    let metadata_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&metadata_filename).exists(), false);

    let drop_table = parse_command(&mut machine, "DROP TABLE IF EXISTS table1");

    assert!(matches!(drop_table, Ok(_result_set)));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    let table = Table::new(database_name, table_name);
    assert_eq!(check_table_exists(&mut machine, &table), false);
    assert!(matches!(machine.actual_database, Some(_database_name)));

    assert_eq!(Path::new(&metadata_filename).exists(), false);
}

