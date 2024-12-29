use rusticodb::machine::context::Context;
use rusticodb::machine::machine::Machine;
use rusticodb::machine::table::Table;
use rusticodb::parser::sql_executor::SqlExecutor;
use rusticodb::setup::setup_system;
use rusticodb::storage::pager::Pager;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_sequence_creation() {
    let context = Context::new();
    let pager = Pager::new();
    let machine = Machine::new(pager, context);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE database1");
    let _ = sql_executor.parse_command("CREATE TABLE table1 (id BIGINT PRIMARY KEY, attr1 INT)");
    let _ = sql_executor.parse_command("CREATE SEQUENCE sequence1 OWNED BY table1.attr1");

    let database_name = String::from("rusticodb");
    let table_name = String::from("sequences");
    assert!(sql_executor.machine.context.check_database_exists(&database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(sql_executor.machine.context.check_table_exists(&table));

    let _ = sql_executor.parse_command("USE rusticodb");
    let result_set = sql_executor.parse_command("SELECT * FROM sequences");

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
        String::from("sequence1")
    );

    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_unsigned_bigint(0, &String::from("next_id")).unwrap(),
        1
    );

}
