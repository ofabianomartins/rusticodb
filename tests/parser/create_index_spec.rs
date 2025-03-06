use rusticodb::machine::Machine;
use rusticodb::machine::Table;
use rusticodb::machine::check_database_exists;
use rusticodb::machine::check_table_exists;

use rusticodb::parser::parse_command;
use rusticodb::setup::setup_system;
use rusticodb::storage::Pager;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_index_creation_if_not_exists() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1 (id BIGINT PRIMARY KEY, attr1 INT)");
    let _ = parse_command(&mut machine, "CREATE INDEX index1 ON table1(attr1)");

    let result_set_create = parse_command(&mut machine, "CREATE INDEX IF NOT EXISTS index1 ON table1(attr1)");

    assert!(matches!(result_set_create, Ok(ref _result_set)));

    let database_name = String::from("rusticodb");
    let table_name = String::from("sequences");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));

    let _ = parse_command(&mut machine, "USE rusticodb");
    let result_set = parse_command(&mut machine, "SELECT * FROM indexes WHERE name = 'index1'");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 1);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 6);

    assert_eq!(result_set.as_ref().unwrap().get(0).unwrap().line_count(), 1);
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("database_name")).unwrap(),
        String::from("database1")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("table_name")).unwrap(),
        String::from("table1")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("column_name")).unwrap(),
        String::from("attr1")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("name")).unwrap(),
        String::from("index1")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("type")).unwrap(),
        String::from("btree")
    );
}

#[test]
pub fn test_index_creation_if_exists() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1 (id BIGINT PRIMARY KEY, attr1 INT)");
    let _ = parse_command(&mut machine, "CREATE INDEX index1 ON table1(attr1)");

    let result_set_create = parse_command(&mut machine, "CREATE INDEX index1 ON table1(attr1)");

    assert!(matches!(result_set_create, Err(ref _result_set)));

    let database_name = String::from("rusticodb");
    let table_name = String::from("sequences");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));

    let _ = parse_command(&mut machine, "USE rusticodb");
    let result_set = parse_command(&mut machine, "SELECT * FROM indexes WHERE name = 'index1'");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 1);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 6);

    assert_eq!(result_set.as_ref().unwrap().get(0).unwrap().line_count(), 1);
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("database_name")).unwrap(),
        String::from("database1")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("table_name")).unwrap(),
        String::from("table1")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("column_name")).unwrap(),
        String::from("attr1")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("name")).unwrap(),
        String::from("index1")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("type")).unwrap(),
        String::from("btree")
    );
}

#[test]
pub fn test_index_creation() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1 (id BIGINT PRIMARY KEY, attr1 INT)");
    let _ = parse_command(&mut machine, "CREATE INDEX index1 ON table1(attr1)");

    let database_name = String::from("rusticodb");
    let table_name = String::from("sequences");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));

    let _ = parse_command(&mut machine, "USE rusticodb");
    let result_set = parse_command(&mut machine, "SELECT * FROM indexes WHERE name = 'index1'");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 1);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 6);

    assert_eq!(result_set.as_ref().unwrap().get(0).unwrap().line_count(), 1);
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("database_name")).unwrap(),
        String::from("database1")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("table_name")).unwrap(),
        String::from("table1")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("column_name")).unwrap(),
        String::from("attr1")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("name")).unwrap(),
        String::from("index1")
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_string(0, &String::from("type")).unwrap(),
        String::from("btree")
    );
}
