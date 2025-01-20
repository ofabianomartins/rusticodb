
use rusticodb::machine::Machine;
use rusticodb::parser::sql_executor::SqlExecutor;
use rusticodb::setup::setup_system;
use rusticodb::storage::pager::Pager;
use rusticodb::utils::ExecutionError;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_in_two_rows_varchar_and_update() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1(id INTEGER PRIMARY KEY, name1 VARCHAR, name2 VARCHAR)");
    let _ = sql_executor.parse_command("INSERT table1 VALUES (\"fabiano\", \"martins\")");
    let _ = sql_executor.parse_command("INSERT table1 VALUES (\"fabiano2\", \"martins2\")");

    let result_set = sql_executor.parse_command("UPDATE table1 SET name1 = \"fabiano3\"");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = sql_executor.parse_command("SELECT * FROM table1 WHERE name1 = \"fabiano3\"");

    assert!(matches!(result_set_select, Ok(ref _result_set)));
    assert_eq!(result_set_select.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set_select.unwrap()[0].column_count(), 2);
}
