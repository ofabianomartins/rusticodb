use std::path::Path;

use rusticodb::config::Config;

use rusticodb::machine::Machine;
use rusticodb::machine::Table;
use rusticodb::machine::check_table_exists;
use rusticodb::machine::check_database_exists;

use rusticodb::utils::execution_error::ExecutionError;
use rusticodb::parser::sql_executor::SqlExecutor;
use rusticodb::setup::setup_system;

use rusticodb::storage::pager::Pager;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_metadata_file() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name, table_name);
    assert!(check_table_exists(&mut sql_executor.machine, &table));
    assert!(matches!(sql_executor.machine.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_without_set_database() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
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
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name, table_name);
    assert_eq!(check_table_exists(&mut sql_executor.machine, &table), false);
    assert!(matches!(sql_executor.machine.actual_database, None));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&table_filename).exists(), false);
}

#[test]
pub fn test_that_already_exists() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
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
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name, table_name);
    assert!(check_table_exists(&mut sql_executor.machine, &table));
    assert!(matches!(sql_executor.machine.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_with_if_not_exists() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
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
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name, table_name);
    assert!(check_table_exists(&mut sql_executor.machine, &table));
    assert!(matches!(sql_executor.machine.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_with_two_varchar_columns() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(name1 VARCHAR, name2 VARCHAR)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut sql_executor.machine, &table));
    assert!(matches!(sql_executor.machine.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_with_two_varchar_columns_and_one_is_not_null() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(name1 VARCHAR NOT NULL, name2 VARCHAR NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut sql_executor.machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("not_null")).unwrap(),
        true
    );
    assert_eq!(
        result_set.unwrap().get(0).unwrap().get_boolean(0, &String::from("not_null")).unwrap(),
        true
    );
}

#[test]
pub fn test_with_two_columns_one_is_int_and_other_is_varchar() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(name1 INTEGER NOT NULL, name2 VARCHAR NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut sql_executor.machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("type")).unwrap(),
        String::from("INT")
    );
    assert_eq!(
        result_set.unwrap().get(0).unwrap().get_string(1, &String::from("type")).unwrap(),
        String::from("VARCHAR")
    );
}

#[test]
pub fn test_with_two_columns_one_is_int_and_other_is_text() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(name1 INTEGER NOT NULL, name2 TEXT NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut sql_executor.machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("type")).unwrap(),
        String::from("INT")
    );
    assert_eq!(
        result_set.unwrap().get(0).unwrap().get_string(1, &String::from("type")).unwrap(),
        String::from("TEXT")
    );
}

#[test]
pub fn test_with_two_columns_one_is_int_and_other_is_boolean() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(name1 INTEGER NOT NULL, name2 BOOLEAN NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut sql_executor.machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("type")).unwrap(),
        String::from("INT")
    );
    assert_eq!(
        result_set.unwrap().get(0).unwrap().get_string(1, &String::from("type")).unwrap(),
        String::from("TINYINT")
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_tinyint() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(name1 TINYINT PRIMARY KEY, name2 VARCHAR NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut sql_executor.machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("type")).unwrap(),
        String::from("TINYINT")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("not_null")).unwrap(),
        true
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("unique")).unwrap(),
        true
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("primary_key")).unwrap(),
        true
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_mediumint() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(name1 MEDIUMINT PRIMARY KEY, name2 VARCHAR NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut sql_executor.machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("type")).unwrap(),
        String::from("INT")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("not_null")).unwrap(),
        true
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("unique")).unwrap(),
        true
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("primary_key")).unwrap(),
        true
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_smallint() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(name1 SMALLINT PRIMARY KEY, name2 VARCHAR NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut sql_executor.machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("type")).unwrap(),
        String::from("SMALLINT")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("not_null")).unwrap(),
        true
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("unique")).unwrap(),
        true
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("primary_key")).unwrap(),
        true
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_int() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(name1 INTEGER PRIMARY KEY, name2 VARCHAR NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut sql_executor.machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("type")).unwrap(),
        String::from("INT")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("not_null")).unwrap(),
        true
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("unique")).unwrap(),
        true
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("primary_key")).unwrap(),
        true
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_bigint() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(name1 BIGINT PRIMARY KEY, name2 VARCHAR NOT NULL)");

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("type")).unwrap(),
        String::from("BIGINT")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("not_null")).unwrap(),
        true
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("unique")).unwrap(),
        true
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_boolean(0, &String::from("primary_key")).unwrap(),
        true
    );
}
