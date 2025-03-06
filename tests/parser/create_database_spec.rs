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
pub fn test_create_database_metadata_file_database1() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let result_set = parse_command(&mut machine, "CREATE DATABASE database1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(check_database_exists(&mut machine, &database_name));
    assert_eq!(matches!(machine.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_database_with_if_not_exists() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let result_set = parse_command(&mut machine, "CREATE DATABASE IF NOT EXISTS database1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(check_database_exists(&mut machine, &database_name));
    assert_eq!(matches!(machine.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_database_with_if_not_exists_in_wrong_order() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let error_parse = parse_command(&mut machine, "CREATE IF NOT EXISTS DATABASE database1");

    assert!(
        matches!(
            error_parse,
            Err(ExecutionError::ParserError(_))
        )
    );

    let database_name = String::from("database1");
    assert!(check_database_exists(&mut machine, &database_name));
    assert_eq!(matches!(machine.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_two_databases() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let result_set = parse_command(&mut machine, "CREATE DATABASE database1; CREATE DATABASE database2");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(check_database_exists(&mut machine, &database_name));

    let database_name = String::from("database2");
    assert!(check_database_exists(&mut machine, &database_name));

    assert_eq!(matches!(machine.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_database_that_already_exists() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let error_parse = parse_command(&mut machine, "CREATE DATABASE database1");

    assert!(matches!(error_parse, Err(ExecutionError::DatabaseExists(_result_set))));

    let database_name = String::from("database1");
    assert!(check_database_exists(&mut machine, &database_name));
    assert_eq!(matches!(machine.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}
