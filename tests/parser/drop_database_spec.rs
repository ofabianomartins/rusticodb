use std::path::Path;

use rusticodb::config::Config;
use rusticodb::machine::Machine;
use rusticodb::machine::check_database_exists;

use rusticodb::utils::execution_error::ExecutionError;

use rusticodb::parser::parse_command;

use rusticodb::setup::setup_system;
use rusticodb::storage::Pager;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_drop_database_metadata_file_database1() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let create_database = parse_command(&mut machine, "CREATE DATABASE database1");

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert_eq!(Path::new(&metadata_foldername).exists(), true);

    let drop_database = parse_command(&mut machine, "DROP DATABASE database1");

    assert!(matches!(create_database, Ok(_result_set)));
    assert!(matches!(drop_database, Ok(_result_set)));

    let database_name = String::from("database1");
    assert_eq!(check_database_exists(&mut machine, &database_name), false);
    assert_eq!(matches!(machine.actual_database, Some(_database_name)), false);

    assert_eq!(Path::new(&metadata_foldername).exists(), false);
}

#[test]
pub fn test_drop_database_does_not_exists() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let drop_database = parse_command(&mut machine, "DROP DATABASE database1");

    assert!(matches!(drop_database, Err(ExecutionError::DatabaseNotExists(_))));

    let database_name = String::from("database1");
    assert_eq!(check_database_exists(&mut machine, &database_name), false);
    assert_eq!(matches!(machine.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert_eq!(Path::new(&metadata_foldername).exists(), false);
}

#[test]
pub fn test_drop_database_does_not_exists_but_use_if_exists() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let drop_database = parse_command(&mut machine, "DROP DATABASE IF EXISTS database1");

    assert!(matches!(drop_database, Ok(_)));

    let database_name = String::from("database1");
    assert_eq!(check_database_exists(&mut machine, &database_name), false);
    assert_eq!(matches!(machine.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert_eq!(Path::new(&metadata_foldername).exists(), false);
}
