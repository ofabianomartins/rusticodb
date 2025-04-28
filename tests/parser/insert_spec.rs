use rusticodb::machine::Machine;
use rusticodb::machine::PagerManager;
use rusticodb::parser::parse_command;
use rusticodb::setup::setup_system;
use rusticodb::utils::ExecutionError;
use rusticodb::storage::Data;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_in_two_varchar_columns() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(name1 VARCHAR, name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1 VALUES (\'fabiano\', \'martins\')");


    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));
    assert_eq!(result_set_select.as_ref().unwrap()[0].tuples.len(), 1);
    assert_eq!(result_set_select.unwrap()[0].column_count(), 2);
}

#[test]
pub fn test_in_two_columns_varchar_and_with_primary_key() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 VARCHAR, name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1(name1, name2) VALUES (\'fabiano\', \'martins\')");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 1);
    assert_eq!(rs[0].column_count(), 3);

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("id")).unwrap(),
        Data::UnsignedBigint(1u64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name1")).unwrap(),
        Data::Varchar(String::from("fabiano"))
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name2")).unwrap(),
        Data::Varchar(String::from("martins"))
    );
}

#[test]
pub fn test_in_two_rows_with_null_value() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 VARCHAR NOT NULL, name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1(name1, name2) VALUES (NULL, \'martins\')");

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 0);
    assert_eq!(rs[0].column_count(), 3);

    assert!(matches!(result_set, Err(ExecutionError::ColumnCantBeNull(_result_set, _, _))));
}

#[test]
pub fn test_in_two_columns_and_one_accept_null_value() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 VARCHAR, name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1(name2) VALUES (\'martins\')");

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 1);
    assert_eq!(rs[0].column_count(), 3);

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("id")).unwrap(),
        Data::UnsignedBigint(1u64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name2")).unwrap(),
        Data::Varchar(String::from("martins"))
    );

    assert!(matches!(result_set, Ok(_result_set)));
}

#[test]
pub fn test_in_two_columns_one_with_default_varchar_value() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 VARCHAR NOT NULL DEFAULT 'fabiano1', name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1(name2) VALUES (\'martins\')");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 1);
    assert_eq!(rs[0].column_count(), 3);

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("id")).unwrap(),
        Data::UnsignedBigint(1u64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name1")).unwrap(),
        Data::Varchar(String::from("fabiano1"))
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name2")).unwrap(),
        Data::Varchar(String::from("martins"))
    );
}

#[test]
pub fn test_in_two_columns_one_with_default_text_value() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 TEXT NOT NULL DEFAULT 'fabiano1', name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1(name2) VALUES (\'martins\')");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 1);
    assert_eq!(rs[0].column_count(), 3);

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("id")).unwrap(),
        Data::UnsignedBigint(1u64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name1")).unwrap(),
        Data::Varchar(String::from("fabiano1"))
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name2")).unwrap(),
        Data::Varchar(String::from("martins"))
    );
}

#[test]
pub fn test_in_two_columns_one_with_default_unsigned_bigint_value() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 BIGINT UNSIGNED NOT NULL DEFAULT 1 , name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1(name2) VALUES (\'martins\')");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 1);
    assert_eq!(rs[0].column_count(), 3);

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("id")).unwrap(),
        Data::UnsignedBigint(1u64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name1")).unwrap(),
        Data::UnsignedBigint(1u64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name2")).unwrap(),
        Data::Varchar(String::from("martins"))
    );
}

#[test]
pub fn test_in_two_columns_one_with_default_unsigned_int_value() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 INT UNSIGNED NOT NULL DEFAULT 1 , name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1(name2) VALUES (\'martins\')");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 1);
    assert_eq!(rs[0].column_count(), 3);

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("id")).unwrap(),
        Data::UnsignedBigint(1u64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name1")).unwrap(),
        Data::UnsignedInt(1u32)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name2")).unwrap(),
        Data::Varchar(String::from("martins"))
    );
}

#[test]
pub fn test_in_two_columns_one_with_default_unsigned_smallint_value() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 SMALLINT UNSIGNED NOT NULL DEFAULT 1 , name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1(name2) VALUES (\'martins\')");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 1);
    assert_eq!(rs[0].column_count(), 3);

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("id")).unwrap(),
        Data::UnsignedBigint(1u64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name1")).unwrap(),
        Data::UnsignedSmallint(1u16)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name2")).unwrap(),
        Data::Varchar(String::from("martins"))
    );
}

#[test]
pub fn test_in_two_columns_one_with_default_unsigned_tinyint_value() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 TINYINT UNSIGNED NOT NULL DEFAULT 1 , name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1(name2) VALUES (\'martins\')");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 1);
    assert_eq!(rs[0].column_count(), 3);

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("id")).unwrap(),
        Data::UnsignedBigint(1u64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name1")).unwrap(),
        Data::UnsignedTinyint(1u8)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name2")).unwrap(),
        Data::Varchar(String::from("martins"))
    );
}

#[test]
pub fn test_in_two_columns_one_with_default_signed_bigint_value() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 BIGINT NOT NULL DEFAULT -1, name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1(name2) VALUES (\'martins\')");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 1);
    assert_eq!(rs[0].column_count(), 3);

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("id")).unwrap(),
        Data::UnsignedBigint(1u64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name1")).unwrap(),
        Data::SignedBigint(-1i64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name2")).unwrap(),
        Data::Varchar(String::from("martins"))
    );
}

#[test]
pub fn test_in_two_columns_one_with_default_signed_int_value() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 INT NOT NULL DEFAULT -1, name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1(name2) VALUES (\'martins\')");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 1);
    assert_eq!(rs[0].column_count(), 3);

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("id")).unwrap(),
        Data::UnsignedBigint(1u64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name1")).unwrap(),
        Data::SignedInt(-1i32)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name2")).unwrap(),
        Data::Varchar(String::from("martins"))
    );
}

#[test]
pub fn test_in_two_columns_one_with_default_signed_smallint_value() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 SMALLINT NOT NULL DEFAULT -1 , name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1(name2) VALUES (\'martins\')");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 1);
    assert_eq!(rs[0].column_count(), 3);

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("id")).unwrap(),
        Data::UnsignedBigint(1u64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name1")).unwrap(),
        Data::SignedSmallint(-1i16)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name2")).unwrap(),
        Data::Varchar(String::from("martins"))
    );
}

#[test]
pub fn test_in_two_columns_one_with_default_signed_tinyint_value() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 TINYINT NOT NULL DEFAULT -1 , name2 VARCHAR)");
    let result_set = parse_command(&mut machine, "INSERT table1(name2) VALUES (\'martins\')");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1");

    assert!(matches!(result_set_select, Ok(ref _result_set)));

    let rs = result_set_select.unwrap();

    assert_eq!(rs[0].tuples.len(), 1);
    assert_eq!(rs[0].column_count(), 3);

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("id")).unwrap(),
        Data::UnsignedBigint(1u64)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name1")).unwrap(),
        Data::SignedTinyint(-1i8)
    );

    assert_eq!(
        rs.get(0).unwrap().get_value(0, &String::from("name2")).unwrap(),
        Data::Varchar(String::from("martins"))
    );
}
