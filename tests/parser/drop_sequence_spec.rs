use rusticodb::machine::Machine;
use rusticodb::machine::Table;
use rusticodb::machine::check_database_exists;
use rusticodb::machine::check_table_exists;

use rusticodb::parser::sql_executor::SqlExecutor;
use rusticodb::setup::setup_system;
use rusticodb::storage::pager::Pager;
use rusticodb::utils::ExecutionError;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_sequence_drop_if_exists() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1 (id BIGINT PRIMARY KEY, attr1 INT)");
    let _ = sql_executor.parse_command("CREATE SEQUENCE sequence1 OWNED BY table1.attr1");

    let result_set_drop = sql_executor.parse_command("DROP SEQUENCE sequence1");

    assert!(matches!(result_set_drop, Ok(ref _result_set)));

    let database_name = String::from("rusticodb");
    let table_name = String::from("sequences");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut sql_executor.machine, &table));

    let _ = sql_executor.parse_command("USE rusticodb");
    let result_set = sql_executor.parse_command("SELECT * FROM indexes WHERE name = 'index1'");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 0);
}

#[test]
pub fn test_sequence_error_if_not_exists() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1 (id BIGINT PRIMARY KEY, attr1 INT)");

    let result_set_drop = sql_executor.parse_command("DROP SEQUENCE sequence1");

    assert!(matches!(result_set_drop, Err(ExecutionError::IndexNotExists(ref _result_set))));

    let database_name = String::from("rusticodb");
    let table_name = String::from("sequences");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut sql_executor.machine, &table));

    let _ = sql_executor.parse_command("USE rusticodb");
    let result_set = sql_executor.parse_command("SELECT * FROM indexes WHERE name = 'index1'");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 0);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 6);
}

#[test]
pub fn test_sequence_drop_with_if_exists_statement() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1 (id BIGINT PRIMARY KEY, attr1 INT)");

    let result_set_drop = sql_executor.parse_command("DROP SEQUENCE IF EXISTS sequence1");

    assert!(matches!(result_set_drop, Ok(ref _result_set)));

    let database_name = String::from("rusticodb");
    let table_name = String::from("sequences");
    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut sql_executor.machine, &table));

    let _ = sql_executor.parse_command("USE rusticodb");
    let result_set = sql_executor.parse_command("SELECT * FROM indexes WHERE name = 'index1'");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 0);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 6);
}
