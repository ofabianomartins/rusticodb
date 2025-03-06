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
pub fn test_use_database_that_not_exists() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let error_parse = parse_command(&mut machine, "USE database1");

    let database_name = String::from("database1");

    assert!(
        matches!(
            error_parse, 
            Err(ExecutionError::DatabaseNotExists(_))
        )
    );

    assert!(matches!(machine.actual_database, None));
    assert_eq!(check_database_exists(&mut machine, &database_name), false);
}

#[test]
pub fn test_use_database_set_in_context() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let result_set = parse_command(&mut machine, "USE database1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(check_database_exists(&mut machine, &database_name));
    assert!(matches!(machine.actual_database,Some(_database_name)));

    let metadata_foldername = format!("{}/{}", Config::data_folder(), database_name);
    assert!(Path::new(&metadata_foldername).exists());
}
