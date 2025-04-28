use std::path::Path;

use rusticodb::config::Config;

use rusticodb::machine::Machine;
use rusticodb::machine::PagerManager;

use rusticodb::parser::parse_command;
use rusticodb::setup::setup_system;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_with_two_columns_and_one_is_a_primary_key() {
    let pager = PagerManager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = parse_command(&mut machine, "CREATE DATABASE database1");
    let _ = parse_command(&mut machine, "USE database1");
    let _ = parse_command(&mut machine, "CREATE TABLE table1(name1 BIGINT PRIMARY KEY, name2 VARCHAR NOT NULL)");

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let _ = parse_command(&mut machine, "USE rusticodb;");
    let result_set_delete = parse_command(&mut machine, "
        DELETE FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set_delete, Ok(ref _result_set)));
    let result_set = parse_command(&mut machine, "
        SELECT * FROM columns WHERE table_name = 'table1' AND database_name = 'database1'
    ");

    assert!(matches!(result_set, Ok(ref _result_set)));
    assert_eq!(result_set.as_ref().unwrap()[0].tuples.len(), 0);
    assert_eq!(result_set.as_ref().unwrap()[0].column_count(), 9);
}
