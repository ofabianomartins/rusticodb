use std::path::Path;

use rusticodb::config::Config;
use rusticodb::machine::Machine;
use rusticodb::parser::sql_executor::SqlExecutor;
use rusticodb::setup::setup_system;
use rusticodb::storage::pager::Pager;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key() {
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
    let result_set_delete = sql_executor.parse_command("
        DELETE FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set_delete, Ok(ref _result_set)));
    let result_set = sql_executor.parse_command("
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 0);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 8);
}
