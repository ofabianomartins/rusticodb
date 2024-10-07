use std::path::Path;

use rusticodb::config::Config;
use rusticodb::machine::context::Context;
use rusticodb::machine::machine::Machine;
use rusticodb::storage::tuple::Tuple;
use rusticodb::setup::setup_system;

use crate::test_utils::destroy_tmp_test_folder;
use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_system_database_setup() {
    let mut machine = Machine::new();
    let mut context = Context::new();

    setup_system(&mut context, &mut machine);

    let metadata_foldername = format!("{}", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());

    let table_filename = format!("{}/{}/", Config::data_folder(), Config::system_database());
    assert!(Path::new(&table_filename).exists());
    assert!(context.check_database_exists(&Config::system_database()));


    let table_filename = format!(
        "{}/{}/{}.db",
        Config::data_folder(), 
        Config::system_database(), 
        Config::system_database_table_databases()
    );
    assert!(Path::new(&table_filename).exists());
    assert!(context.check_table_exists(&Config::system_database(), &Config::system_database_table_databases()));

    let table_filename = format!(
        "{}/{}/{}.db", 
        Config::data_folder(), 
        Config::system_database(),
        Config::system_database_table_tables()
    );
    assert!(Path::new(&table_filename).exists());
    assert!(context.check_table_exists(&Config::system_database(), &Config::system_database_table_tables()));

    let table_filename = format!(
        "{}/{}/{}.db", 
        Config::data_folder(), 
        Config::system_database(),
        Config::system_database_table_columns()
    );
    assert!(Path::new(&table_filename).exists());
    assert!(context.check_table_exists(&Config::system_database(), &Config::system_database_table_columns()));

    destroy_tmp_test_folder();
}

#[test]
pub fn test_system_database_setup_with_data_to_load() {
    let mut machine = Machine::new();
    let mut context = Context::new();

    create_tmp_test_folder();

    machine.create_database(&Config::system_database());
    machine.create_table(&Config::system_database(), &Config::system_database_table_databases());

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuples.push(tuple);
    machine.insert_tuples(&Config::system_database(), &Config::system_database_table_databases(), &mut tuples);

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&String::from("database1"));
    tuples.push(tuple);
    machine.insert_tuples(&Config::system_database(), &Config::system_database_table_databases(), &mut tuples);

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&String::from("database2"));
    tuples.push(tuple);
    machine.insert_tuples(&Config::system_database(), &Config::system_database_table_databases(), &mut tuples);

    setup_system(&mut context, &mut machine);

    assert!(context.check_database_exists(&Config::system_database()));
    assert!(context.check_database_exists(&String::from("database1")));
    assert!(context.check_database_exists(&String::from("database2")));

    destroy_tmp_test_folder();
}

