use std::path::Path;

use rusticodb::config::Config;

use rusticodb::machine::Machine;
use rusticodb::machine::create_database;
use rusticodb::machine::check_database_exists;

use rusticodb::storage::Pager;

use rusticodb::setup::setup_system;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_if_database_exists_is_true() {
    let database1 = String::from("database1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = create_database(&mut machine, database1.clone(), false);
    assert!(check_database_exists(&mut machine, &database1));
}

#[test]
pub fn test_if_database_exists_is_false() {
    let database1 = String::from("database1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    assert_eq!(check_database_exists(&mut machine, &database1), false);
}

#[test]
pub fn test_create_database_metadata_file_database1() {
    let database1 = String::from("database1");
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = create_database(&mut machine, database1.clone(), false);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());
}
