
use rusticodb::machine::Machine;
use rusticodb::parser::parse_command;
use rusticodb::setup::setup_system;
use rusticodb::storage::Pager;
// use rusticodb::utils::ExecutionError;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_in_two_rows_varchar_and_update() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(id BIGINT PRIMARY KEY, name1 VARCHAR, name2 VARCHAR)");
    let _ = parse_command(&mut machine, "INSERT table1 VALUES (1, \'fabiano\', \'martins\')");
    let _ = parse_command(&mut machine, "INSERT table1 VALUES (2, \'fabiano2\', \'martins2\')");

    let result_set = parse_command(&mut machine, "UPDATE table1 SET name1 = \"fabiano3\"");

    assert!(matches!(result_set, Ok(_result_set)));

    let result_set_select = parse_command(&mut machine, "SELECT * FROM table1 WHERE name = \"fabiano3\"");

    assert!(matches!(result_set_select, Ok(ref _result_set)));
    assert_eq!(result_set_select.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set_select.unwrap()[0].column_count(), 3);
}
