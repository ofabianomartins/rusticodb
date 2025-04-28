use rusticodb::machine::Machine;
use rusticodb::machine::Table;
use rusticodb::machine::check_table_exists;
use rusticodb::machine::check_database_exists;
use rusticodb::machine::PagerManager;

use rusticodb::utils::execution_error::ExecutionError;
use rusticodb::parser::parse_command;
use rusticodb::setup::setup_system;

use rusticodb::storage::Data;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_view_creation() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "USE rusticodb");
    let result_set_create = parse_command(&mut machine, "CREATE VIEW view1 AS SELECT * FROM columns");

    assert!(matches!(result_set_create, Ok(ref _result_set)));

    let database_name = String::from("rusticodb");
    let table_name = String::from("tables");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));

    let result_set = parse_command(&mut machine, "SELECT * FROM tables WHERE name = 'view1'");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 1);

    assert_eq!(result_set.as_ref().unwrap().get(0).unwrap().line_count(), 1);
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("database_name")).unwrap(),
        Data::Varchar(String::from("rusticodb"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("name")).unwrap(),
        Data::Varchar(String::from("view1"))
    );
    assert_eq!(
        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("type")).unwrap(),
        Data::Varchar(String::from("view"))
    );
}

#[test]
pub fn test_view_creation_error_when_exists() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "USE rusticodb");
    let _ = parse_command(&mut machine, "CREATE VIEW view1 AS SELECT * FROM columns");

    let result_set_create = parse_command(&mut machine, "CREATE VIEW view1 AS SELECT * FROM columns");

    assert!(matches!(result_set_create, Err(ExecutionError::ViewExists(ref _result_set))));

    let database_name = String::from("rusticodb");
    let table_name = String::from("tables");
    assert!(check_database_exists(&mut machine, &database_name));
    let table = Table::new(database_name.clone(), table_name.clone());
    assert!(check_table_exists(&mut machine, &table));

    let result_set = parse_command(&mut machine, "SELECT * FROM tables WHERE name = 'view1'");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 1);
}

//#[test]
//pub fn test_view_creation_error_with_or_replace() {
//    let pager = PagerManager::new();
//    let machine = Machine::new(pager);
//    let mut sql_executor = SqlExecutor::new(machine);
//
//    create_tmp_test_folder();
//
//    setup_system(&mut sql_executor.machine);
//
//    let _ = parse_command(&mut machine, "USE rusticodb");
//    let _ = parse_command(&mut machine, "CREATE VIEW view1 AS SELECT * FROM columns");
//
//    let result_set_create = parse_command(&mut machine, "CREATE OR REPLACE VIEW view1 AS SELECT * FROM tables");
//
//    assert!(matches!(result_set_create, Ok(ref _result_set)));
//
//    let database_name = String::from("rusticodb");
//    let table_name = String::from("tables");
//    assert!(check_database_exists(&mut sql_executor.machine, &database_name));
//    let table = Table::new(database_name.clone(), table_name.clone());
//    assert!(check_table_exists(&mut sql_executor.machine, &table));
//
//    let result_set = parse_command(&mut machine, "SELECT * FROM tables WHERE name = 'view1'");
//
//    assert!(matches!(result_set, Ok(ref _result_set)));
//    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 1);
//
//    assert_eq!(
//        result_set.as_ref().unwrap().get(0).unwrap().get_value(0, &String::from("query")).unwrap(),
//        String::from("SELECT * FROM tables")
//    );
//}

