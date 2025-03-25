use std::path::Path;
use bincode::deserialize;

use rusticodb::config::Config;

use rusticodb::machine::Machine;
use rusticodb::machine::Table;
use rusticodb::machine::create_database;
use rusticodb::machine::create_table;
use rusticodb::machine::check_table_exists;

use rusticodb::storage::Header;
use rusticodb::storage::Pager;
use rusticodb::storage::read_data;

use rusticodb::setup::setup_system;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_if_table_exists_is_true() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = create_database(&mut machine, database1.clone(), false);
    let table = Table::new(database1.clone(), table1.clone());
    let _ = create_table(&mut machine, &table, Vec::new());
    assert_eq!(check_table_exists(&mut machine, &table), true);
}

#[test]
pub fn test_if_table_exists_is_false() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = create_database(&mut machine, database1.clone(), false);
    let table = Table::new(database1.clone(), table1.clone());
    assert_eq!(check_table_exists(&mut machine, &table), false);
}

#[test]
pub fn test_create_table_metadata_file() {
    let database1 = String::from("database1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let table = Table::new(database1.clone(), String::from("table1"));
    let _ = create_database(&mut machine, database1.clone(), false);
    let _ = create_table(&mut machine, &table, Vec::new());

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    let byte_array = read_data(&table_filename, 0);
    println!("{:?}", byte_array);
    let header: Header = deserialize(&byte_array).expect("Deserialization failed");
    
    assert_eq!(header.page_count, 0);
    assert_eq!(header.next_rowid, 1);
}
