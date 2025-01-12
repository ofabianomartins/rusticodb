use std::path::Path;

use rusticodb::config::Config;
use rusticodb::machine::Machine;
use rusticodb::machine::check_database_exists;

use rusticodb::utils::execution_error::ExecutionError;
use rusticodb::parser::sql_executor::SqlExecutor;
use rusticodb::setup::setup_system;
use rusticodb::storage::pager::Pager;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_use_database_that_not_exists() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let error_parse = sql_executor.parse_command("USE database1");

    let database_name = String::from("database1");

    assert!(
        matches!(
            error_parse, 
            Err(ExecutionError::DatabaseNotExists(_))
        )
    );

    assert!(matches!(sql_executor.machine.actual_database, None));
    assert_eq!(check_database_exists(&mut sql_executor.machine, &database_name), false);
}

#[test]
pub fn test_use_database_set_in_context() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let result_set = sql_executor.parse_command("USE database1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    assert!(matches!(sql_executor.machine.actual_database,Some(_database_name)));

    let metadata_foldername = format!("{}/{}", Config::data_folder(), database_name);
    assert!(Path::new(&metadata_foldername).exists());
}
