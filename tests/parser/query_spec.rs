use rusticodb::machine::Machine;
use rusticodb::machine::check_database_exists;
use rusticodb::utils::execution_error::ExecutionError;
use rusticodb::parser::parse_command;
use rusticodb::setup::setup_system;
use rusticodb::storage::Pager;
use rusticodb::storage::tuple_cell_count;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_select_database_tables() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let use_database = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "SELECT name FROM databases");

    assert!(matches!(use_database, Ok(_result_set)));
    // assert!(matches!(result_set, Ok(ref result_sets)));

    assert!(matches!(result_set.unwrap().get(0).unwrap().get_string(0, &String::from("name")), Ok(_database_name)));

    let database_name = String::from("database1");
    assert!(check_database_exists(&mut machine, &database_name));

    assert!(matches!(machine.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_all_database_tables() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let use_database = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "SELECT * FROM databases");

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
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let use_database = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "SELECT name as atr1 FROM databases");

    assert!(matches!(use_database, Ok(_result_set)));
    // assert!(matches!(result_set, Ok(ref result_sets)));

    assert!(matches!(result_set.unwrap().get(0).unwrap().get_string(0, &String::from("name")), Ok(_database_name)));

    let database_name = String::from("database1");
    assert!(check_database_exists(&mut machine, &database_name));

    assert!(matches!(machine.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_with_defined_wizard_database_tables() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let use_database = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "SELECT columns.*, name FROM columns");

    assert!(matches!(use_database, Ok(_result_set)));
    // assert!(matches!(result_set, Ok(ref result_sets)));

    assert!(matches!(result_set.unwrap().get(0).unwrap().get_string(0, &String::from("name")), Ok(_database_name)));

    let database_name = String::from("database1");
    assert!(check_database_exists(&mut machine, &database_name));

    assert!(matches!(machine.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_with_defined_wizard_and_alias_database_tables() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let use_database = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "SELECT columns.*, name as atr1 FROM columns");

    assert!(matches!(use_database, Ok(_result_set)));
    // assert!(matches!(result_set, Ok(ref result_sets)));

    assert!(matches!(result_set.unwrap().get(0).unwrap().get_string(0, &String::from("name")), Ok(_database_name)));

    let database_name = String::from("database1");
    assert!(check_database_exists(&mut machine, &database_name));

    assert!(matches!(machine.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_with_defined_attr_and_alias_database_tables() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let use_database = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "SELECT columns.name as atr2, name as atr1 FROM columns");

    assert!(matches!(use_database, Ok(_result_set)));
    // assert!(matches!(result_set, Ok(ref result_sets)));

    assert!(matches!(result_set.unwrap().get(0).unwrap().get_string(0, &String::from("name")), Ok(_database_name)));

    let database_name = String::from("database1");
    assert!(check_database_exists(&mut machine, &database_name));

    assert!(matches!(machine.actual_database, Some(_database_name)));
}

#[test]
pub fn test_select_with_wrong_database_that_not_exists() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "SELECT * FROM columns22");

    assert!(matches!(result_set, Err(ExecutionError::TableNotExists(_result_set))));
}

#[test]
pub fn test_select_with_two_tables() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "SELECT * FROM columns a, columns b");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 784);
    assert_eq!(tuple_cell_count(&result_set.unwrap()[0].tuples[0]), 18);
}

#[test]
pub fn test_select_with_three_tables() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "SELECT * FROM columns a, columns b, columns c");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 21952);
    assert_eq!(tuple_cell_count(&result_set.unwrap()[0].tuples[0]), 27);
}

#[test]
pub fn test_select_with_all_and_more_one_attr() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "SELECT *, name FROM columns");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 28);
    assert_eq!(result_set.unwrap()[0].column_count(), 10);
}

#[test]
pub fn test_select_with_limit_clause() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "SELECT * FROM columns LIMIT 2 ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 2);
    assert_eq!(result_set.unwrap()[0].column_count(), 9);
}

#[test]
pub fn test_select_with_all_where_equal() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "SELECT * FROM columns WHERE table_name = 'tables' ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 5);
    assert_eq!(result_set.unwrap()[0].column_count(), 9);
}

#[test]
pub fn test_select_with_all_where_with_not_equal() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, "SELECT * FROM columns WHERE table_name != 'tables' ");


    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 23);
    assert_eq!(result_set.unwrap()[0].column_count(), 9);
}

#[test]
pub fn test_select_with_all_where_with_and_conditions() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine,
        "SELECT * FROM columns WHERE table_name = 'tables' and database_name = 'rusticodb'"
    );

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 5);
    assert_eq!(result_set.unwrap()[0].column_count(), 9);
}


#[test]
pub fn test_select_with_all_where_with_or_conditions() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set = parse_command(&mut machine, 
        "SELECT * FROM columns WHERE table_name = 'tables' or table_name = 'databases'"
    );

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 7);
    assert_eq!(result_set.unwrap()[0].column_count(), 9);
}
