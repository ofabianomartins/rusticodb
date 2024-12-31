use rusticodb::machine::machine::Machine;
use rusticodb::utils::execution_error::ExecutionError;
use rusticodb::parser::sql_executor::SqlExecutor;
use rusticodb::setup::setup_system;
use rusticodb::storage::pager::Pager;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_select_database_tables() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
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
    assert!(sql_executor.machine.check_database_exists(&database_name));

    assert!(matches!(sql_executor.machine.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_all_database_tables() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let use_database = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT * FROM databases");

    assert!(matches!(use_database, Ok(_result_set)));
    let rs = result_set.unwrap();

    assert_eq!(rs.get(0).unwrap().line_count(), 1);
    assert_eq!(
        rs.get(0).unwrap().get_string(0, &String::from("name")).unwrap(),
        String::from("rusticodb")
    );
}

#[test]
pub fn test_select_with_alias_database_tables() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
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
    assert!(sql_executor.machine.check_database_exists(&database_name));

    assert!(matches!(sql_executor.machine.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_with_defined_wizard_database_tables() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
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
    assert!(sql_executor.machine.check_database_exists(&database_name));

    assert!(matches!(sql_executor.machine.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_with_defined_wizard_and_alias_database_tables() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
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
    assert!(sql_executor.machine.check_database_exists(&database_name));

    assert!(matches!(sql_executor.machine.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_with_defined_attr_and_alias_database_tables() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
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
    assert!(sql_executor.machine.check_database_exists(&database_name));

    assert!(matches!(sql_executor.machine.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_with_wrong_database_that_not_exists() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("CREATE DATABASE database1");
    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT * FROM columns22");

    assert!(matches!(result_set, Err(ExecutionError::TableNotExists(_result_set))));
}

#[test]
pub fn test_select_with_two_tables() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT * FROM columns a, columns b");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 361);
    assert_eq!(result_set.unwrap()[0].tuples[0].cell_count(), 16);
}

#[test]
pub fn test_select_with_three_tables() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT * FROM columns a, columns b, columns c");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 6859);
    assert_eq!(result_set.unwrap()[0].tuples[0].cell_count(), 24);
}

#[test]
pub fn test_select_with_all_and_more_one_attr() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT *, name FROM columns");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 19);
    assert_eq!(result_set.unwrap()[0].column_count(), 9);
}

#[test]
pub fn test_select_with_limit_clause() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT * FROM columns LIMIT 2 ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.unwrap()[0].column_count(), 8);
}

#[test]
pub fn test_select_with_all_where_equal() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT * FROM columns WHERE table_name = 'tables' ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 3);
    assert_eq!(result_set.unwrap()[0].column_count(), 8);
}

#[test]
pub fn test_select_with_all_where_with_not_equal() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command("SELECT * FROM columns WHERE table_name != 'tables' ");


    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 16);
    assert_eq!(result_set.unwrap()[0].column_count(), 8);
}

#[test]
pub fn test_select_with_all_where_with_and_conditions() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command(
        "SELECT * FROM columns WHERE table_name = 'tables' and database_name = 'rusticodb'"
    );

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 3);
    assert_eq!(result_set.unwrap()[0].column_count(), 8);
}


#[test]
pub fn test_select_with_all_where_with_or_conditions() {
    let pager = Pager::new();
    let machine = Machine::new(pager);
    let mut sql_executor = SqlExecutor::new(machine);

    create_tmp_test_folder();

    setup_system(&mut sql_executor.machine);

    let _ = sql_executor.parse_command("USE rusticodb;");
    let result_set = sql_executor.parse_command(
        "SELECT * FROM columns WHERE table_name = 'tables' or table_name = 'databases'"
    );

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 5);
    assert_eq!(result_set.unwrap()[0].column_count(), 8);
}
