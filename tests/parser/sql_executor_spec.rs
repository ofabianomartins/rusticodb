use std::path::Path;

use rusticodb::config::Config;
use rusticodb::machine::context::Context;
use rusticodb::machine::machine::Machine;
use rusticodb::machine::result_set::ExecutionError;
use rusticodb::parser::sql_executor::SqlExecutor;
use rusticodb::setup::setup_system;
use rusticodb::storage::pager::Pager;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_create_database_metadata_file_database1() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let result_set = sql_executor.parse_command("CREATE DATABASE database1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert_eq!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_database_with_if_not_exists() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let result_set = sql_executor.parse_command("CREATE DATABASE IF NOT EXISTS database1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert_eq!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_database_with_if_not_exists_in_wrong_order() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let error_parse = sql_executor.parse_command("CREATE IF NOT EXISTS DATABASE database1");

    assert!(
        matches!(
            error_parse,
            Err(ExecutionError::ParserError(_))
        )
    );

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert_eq!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_two_databases() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let result_set = sql_executor.parse_command("CREATE DATABASE database1; CREATE DATABASE database2");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));

    let database_name = String::from("database2");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));

    assert_eq!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_database_that_already_exists() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let error_parse = sql_executor.parse_command("CREATE DATABASE database1");

    assert!(matches!(error_parse, Err(ExecutionError::DatabaseExists(_result_set))));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert_eq!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_drop_database_metadata_file_database1() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let create_database = sql_executor.parse_command("CREATE DATABASE database1");

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert_eq!(Path::new(&metadata_foldername).exists(), true);

    let drop_database = sql_executor.parse_command("DROP DATABASE database1");

    assert!(matches!(create_database, Ok(_result_set)));
    assert!(matches!(drop_database, Ok(_result_set)));

    let database_name = String::from("database1");
    assert_eq!(sql_executor.machine.context.check_database_exists(&database_name), false);
    assert_eq!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)), false);

    assert_eq!(Path::new(&metadata_foldername).exists(), false);
}

#[test]
pub fn test_drop_database_does_not_exists() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let drop_database = sql_executor.parse_command("DROP DATABASE database1");

    assert!(matches!(drop_database, Err(ExecutionError::DatabaseNotExists(_))));

    let database_name = String::from("database1");
    assert_eq!(sql_executor.machine.context.check_database_exists(&database_name), false);
    assert_eq!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert_eq!(Path::new(&metadata_foldername).exists(), false);
}

#[test]
pub fn test_drop_database_does_not_exists_but_use_if_exists() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let drop_database = sql_executor.parse_command("DROP DATABASE IF EXISTS database1");

    assert!(matches!(drop_database, Ok(_)));

    let database_name = String::from("database1");
    assert_eq!(sql_executor.machine.context.check_database_exists(&database_name), false);
    assert_eq!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert_eq!(Path::new(&metadata_foldername).exists(), false);
}

#[test]
pub fn test_use_database_that_not_exists() {
    let database_name = String::from("database1");

    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let error_parse = sql_executor.parse_command("USE database1");

    assert!(
        matches!(
            error_parse, 
            Err(ExecutionError::DatabaseNotExists(_))
        )
    );

    assert!(matches!(sql_executor.machine.context.actual_database, None));
    assert_eq!(sql_executor.machine.context.check_database_exists(&database_name), false);
}

#[test]
pub fn test_use_database_set_in_context() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let result_set = sql_executor.parse_command("USE database1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert!(
        matches!(
            sql_executor.machine.context.actual_database, 
            Some(_database_name)
        )
    );

    let metadata_foldername = format!("{}/{}", Config::data_folder(), database_name);
    assert!(Path::new(&metadata_foldername).exists());
}

#[test]
pub fn test_create_table_metadata_file() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert!(sql_executor.machine.context.check_table_exists(&database_name, &table_name));
    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_create_table_without_set_database() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let error_parse = sql_executor.parse_command("CREATE TABLE table1");

    assert!(
        matches!(
            error_parse,
            Err(ExecutionError::DatabaseNotSetted)
        )
    );

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert_eq!(sql_executor.machine.context.check_table_exists(&database_name, &table_name), false);
    assert!(matches!(sql_executor.machine.context.actual_database, None));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&table_filename).exists(), false);
}

#[test]
pub fn test_create_table_that_already_exists() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1");
    let error_parse = sql_executor.parse_command("CREATE TABLE table1");

    assert!(matches!(error_parse, Err(ExecutionError::DatabaseExists(_result_set))));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert!(sql_executor.machine.context.check_table_exists(&database_name, &table_name));
    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_create_table_with_if_not_exists() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1");
    let result_set = sql_executor.parse_command("CREATE TABLE IF NOT EXISTS table1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert!(sql_executor.machine.context.check_table_exists(&database_name, &table_name));
    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_create_table_with_two_columns() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(name1 VARCHAR, name2 VARCHAR)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    let column_name1 = String::from("name1");
    let column_name2 = String::from("name2");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    assert!(sql_executor.machine.context.check_table_exists(&database_name, &table_name));
    assert!(sql_executor.machine.context.check_column_exists(&database_name, &table_name, &column_name1));
    assert!(sql_executor.machine.context.check_column_exists(&database_name, &table_name, &column_name2));
    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_drop_table_metadata_file_table1() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1");

    let metadata_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&metadata_filename).exists(), true);

    let drop_table = sql_executor.parse_command("DROP TABLE table1");

    assert!(matches!(drop_table, Ok(_result_set)));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert_eq!(sql_executor.machine.context.check_table_exists(&database_name, &table_name), false);
    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));

    assert_eq!(Path::new(&metadata_filename).exists(), false);
}

#[test]
pub fn test_drop_table_metadata_that_not_exists() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");

    let metadata_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&metadata_filename).exists(), false);

    let drop_table = sql_executor.parse_command("DROP TABLE table1");

    assert!(matches!(drop_table, Err(ExecutionError::TableNotExists(_result_set))));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert_eq!(sql_executor.machine.context.check_table_exists(&database_name, &table_name), false);
    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));

    assert_eq!(Path::new(&metadata_filename).exists(), false);
}

#[test]
pub fn test_drop_table_if_exists() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");

    let metadata_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&metadata_filename).exists(), false);

    let drop_table = sql_executor.parse_command("DROP TABLE IF EXISTS table1");

    assert!(matches!(drop_table, Ok(_result_set)));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert_eq!(sql_executor.machine.context.check_table_exists(&database_name, &table_name), false);
    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));

    assert_eq!(Path::new(&metadata_filename).exists(), false);
}

#[test]
pub fn test_select_database_tables() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let use_database = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT name FROM databases");

    assert!(matches!(use_database, Ok(_result_set)));
    // assert!(matches!(result_set, Ok(ref result_sets)));

    assert!(matches!(result_set.unwrap().get(0).unwrap().get_string(0, &String::from("name")), Ok(_database_name)));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));

    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_all_database_tables() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let use_database = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT * FROM databases");

    assert!(matches!(use_database, Ok(_result_set)));
    // assert!(matches!(result_set, Ok(ref result_sets)));

    assert!(matches!(result_set.unwrap().get(0).unwrap().get_string(0, &String::from("name")), Ok(_database_name)));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));

    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_with_alias_database_tables() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let use_database = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT name as atr1 FROM databases");

    assert!(matches!(use_database, Ok(_result_set)));
    // assert!(matches!(result_set, Ok(ref result_sets)));

    assert!(matches!(result_set.unwrap().get(0).unwrap().get_string(0, &String::from("name")), Ok(_database_name)));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));

    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_with_defined_wizard_database_tables() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let use_database = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT columns.*, name FROM columns");

    assert!(matches!(use_database, Ok(_result_set)));
    // assert!(matches!(result_set, Ok(ref result_sets)));

    assert!(matches!(result_set.unwrap().get(0).unwrap().get_string(0, &String::from("name")), Ok(_database_name)));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));

    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_with_defined_wizard_and_alias_database_tables() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let use_database = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT columns.*, name as atr1 FROM columns");

    assert!(matches!(use_database, Ok(_result_set)));
    // assert!(matches!(result_set, Ok(ref result_sets)));

    assert!(matches!(result_set.unwrap().get(0).unwrap().get_string(0, &String::from("name")), Ok(_database_name)));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));

    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_with_defined_attr_and_alias_database_tables() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let use_database = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT columns.name as atr2, name as atr1 FROM columns");

    assert!(matches!(use_database, Ok(_result_set)));
    // assert!(matches!(result_set, Ok(ref result_sets)));

    assert!(matches!(result_set.unwrap().get(0).unwrap().get_string(0, &String::from("name")), Ok(_database_name)));

    let database_name = String::from("database1");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));

    assert!(matches!(sql_executor.machine.context.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_with_wrong_database_that_not_exists() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT * FROM columns22");

    assert!(matches!(result_set, Err(ExecutionError::TableNotExists(_result_set))));
}

