use std::path::Path;

use rusticodb::config::Config;

use rusticodb::machine::Machine;
use rusticodb::machine::Table;
use rusticodb::machine::check_table_exists;
use rusticodb::machine::check_database_exists;
use rusticodb::machine::PagerManager;

use rusticodb::utils::ExecutionError;
use rusticodb::parser::parse_command;
use rusticodb::setup::setup_system;

use rusticodb::storage::Data;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_metadata_file() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name, table_name);
    assert!(check_table_exists(&mut machine, &table));
    assert!(matches!(machine.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_without_set_database() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let error_parse = parse_command(&mut machine, "CREATE TABLE table1");

    assert!(
        matches!(
            error_parse,
            Err(ExecutionError::DatabaseNotSetted)
        )
    );

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name, table_name);
    assert_eq!(check_table_exists(&mut machine, &table), false);
    assert!(matches!(machine.actual_database, None));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert_eq!(Path::new(&table_filename).exists(), false);
}

#[test]
pub fn test_that_already_exists() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let create_table_result = parse_command(&mut machine, "CREATE TABLE table1");
    let error_parse = parse_command(&mut machine, "CREATE TABLE table1");

    assert!(matches!(create_table_result, Ok(_)));
    assert!(matches!(error_parse, Err(ExecutionError::DatabaseExists(_result_set))));

    let database_name = String::from("database1");
    let table_name = String::from("table1");

    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name, table_name);
    assert!(check_table_exists(&mut machine, &table));
    assert!(matches!(machine.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_with_if_not_exists() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1");
    let result_set = parse_command(&mut machine, "CREATE TABLE IF NOT EXISTS table1");

    assert!(matches!(result_set, Ok(_result_set)));

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name, table_name);
    assert!(check_table_exists(&mut machine, &table));
    assert!(matches!(machine.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_with_two_varchar_columns() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(name1 VARCHAR, name2 VARCHAR)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));
    assert!(matches!(machine.actual_database, Some(_database_name)));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());
}

#[test]
pub fn test_with_two_varchar_columns_and_one_is_not_null() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(name1 VARCHAR NOT NULL, name2 VARCHAR NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.unwrap().get(0).unwrap().get_value(0, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
}

#[test]
pub fn test_with_two_columns_one_is_int_and_other_is_varchar() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(name1 INTEGER NOT NULL, name2 VARCHAR NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("type")).unwrap(),
        Data::Varchar(String::from("SIGNED INT"))
    );
    assert_eq!(
        result_set.unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("VARCHAR"))
    );
}

#[test]
pub fn test_with_two_columns_one_is_int_and_other_is_text() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(name1 INTEGER NOT NULL, name2 TEXT NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("type")).unwrap(),
        Data::Varchar(String::from("SIGNED INT"))
    );
    assert_eq!(
        result_set.unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("TEXT"))
    );
}

#[test]
pub fn test_with_two_columns_one_is_int_and_other_is_boolean() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(name1 INTEGER NOT NULL, name2 BOOLEAN NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("type")).unwrap(),
        Data::Varchar(String::from("SIGNED INT"))
    );
    assert_eq!(
        result_set.unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED TINYINT"))
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_unsigned_tinyint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(name1 TINYINT PRIMARY KEY, name2 VARCHAR NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED TINYINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("unique")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("primary_key")).unwrap(),
        Data::Boolean(true)
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_unsigned_mediumint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(name1 MEDIUMINT PRIMARY KEY, name2 VARCHAR NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED INT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("unique")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("primary_key")).unwrap(),
        Data::Boolean(true)
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_unsigned_smallint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(name1 SMALLINT PRIMARY KEY, name2 VARCHAR NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED SMALLINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("unique")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("primary_key")).unwrap(),
        Data::Boolean(true)
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_unsigned_int() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(name1 INTEGER PRIMARY KEY, name2 VARCHAR NOT NULL)");

    let database_name = String::from("database1");
    let table_name = String::from("table1");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED INT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("unique")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("primary_key")).unwrap(),
        Data::Boolean(true)
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_unsigned_bigint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(name1 BIGINT PRIMARY KEY, name2 VARCHAR NOT NULL)");

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED BIGINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("unique")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("primary_key")).unwrap(),
        Data::Boolean(true)
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_unsigned_tinyint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 TINYINT UNSIGNED NOT NULL)");

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED TINYINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_signed_tinyint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 TINYINT NOT NULL)");

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("SIGNED TINYINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_unsigned_smallint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 SMALLINT UNSIGNED NOT NULL)");

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED SMALLINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_signed_smallint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 SMALLINT NOT NULL)");

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("SIGNED SMALLINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_unsigned_int() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 INT UNSIGNED NOT NULL)");

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED INT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_signed_int() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 INT NOT NULL)");

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("SIGNED INT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_unsigned_bigint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 BIGINT UNSIGNED NOT NULL)");

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED BIGINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_signed_bigint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 BIGINT NOT NULL)");

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("SIGNED BIGINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_has_default_value_unsigned_bigint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, 
        "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 BIGINT UNSIGNED NOT NULL DEFAULT 1)"
    );

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED BIGINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("default")).unwrap(),
        Data::Varchar(String::from("1"))
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_has_default_value_signed_bigint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, 
        "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 BIGINT NOT NULL DEFAULT -1)"
    );

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("SIGNED BIGINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("default")).unwrap(),
        Data::Varchar(String::from("-1"))
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_has_default_value_unsigned_int() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, 
        "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 INT UNSIGNED NOT NULL DEFAULT 1)"
    );

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED INT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("default")).unwrap(),
        Data::Varchar(String::from("1"))
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_has_default_value_signed_int() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, 
        "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 INT NOT NULL DEFAULT -1)"
    );

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("SIGNED INT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("default")).unwrap(),
        Data::Varchar(String::from("-1"))
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_has_default_value_unsigned_smallint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, 
        "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 SMALLINT UNSIGNED NOT NULL DEFAULT 1)"
    );

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED SMALLINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("default")).unwrap(),
        Data::Varchar(String::from("1"))
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_has_default_value_signed_smallint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, 
        "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 SMALLINT NOT NULL DEFAULT -1)"
    );

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("SIGNED SMALLINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("default")).unwrap(),
        Data::Varchar(String::from("-1"))
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_has_default_value_unsigned_tinyint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, 
        "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 TINYINT UNSIGNED NOT NULL DEFAULT 1)"
    );

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED TINYINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("default")).unwrap(),
        Data::Varchar(String::from("1"))
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_has_default_value_signed_tinyint() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, 
        "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 TINYINT NOT NULL DEFAULT -1)"
    );

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("SIGNED TINYINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("default")).unwrap(),
        Data::Varchar(String::from("-1"))
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_has_default_value_string() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, 
        "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 VARCHAR NOT NULL DEFAULT 'test1')"
    );

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("VARCHAR"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("default")).unwrap(),
        Data::Varchar(String::from("test1"))
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_has_default_value_text() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, 
        "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 TEXT NOT NULL DEFAULT 'test1')"
    );

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("TEXT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("default")).unwrap(),
        Data::Varchar(String::from("test1"))
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_has_default_value_true_boolean() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, 
        "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 BOOLEAN NOT NULL DEFAULT true)"
    );

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED TINYINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("default")).unwrap(),
        Data::Varchar(String::from("1"))
    );
}

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key_and_second_has_default_value_false_boolean() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let result_create = parse_command(&mut machine, 
        "CREATE TABLE table1(id BIGINT PRIMARY KEY, name2 BOOLEAN NOT NULL DEFAULT false)"
    );

    assert!(matches!(result_create, Ok(ref _result_set)));

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("type")).unwrap(),
        Data::Varchar(String::from("UNSIGNED TINYINT"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("not_null")).unwrap(),
        Data::Boolean(true)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("unique")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("primary_key")).unwrap(),
        Data::Boolean(false)
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(1, &String::from("default")).unwrap(),
        Data::Varchar(String::from("0"))
    );
}
