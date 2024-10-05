use std::path::Path;

use rusticodb::config::Config;
use rusticodb::storage::cell::Cell;
use rusticodb::storage::cell::CellType;
use rusticodb::storage::tuple::Tuple;
use rusticodb::storage::machine::Machine;
use rusticodb::storage::os_interface::BLOCK_SIZE;

use crate::test_utils::create_tmp_test_folder;
use crate::test_utils::destroy_tmp_test_folder;
use crate::test_utils::read_from_file;

#[test]
pub fn test_if_database_exists_is_true() {
    let database1 = String::from("database1");
    let mut machine = Machine::new();

    create_tmp_test_folder();

    machine.create_database(&database1);
    assert!(machine.database_exists(&database1));

    destroy_tmp_test_folder();
}

#[test]
pub fn test_if_database_exists_is_false() {
    let database1 = String::from("database1");
    let mut machine = Machine::new();

    create_tmp_test_folder();

    assert_eq!(machine.database_exists(&database1), false);

    destroy_tmp_test_folder();
}

#[test]
pub fn test_if_table_exists_is_true() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let mut machine = Machine::new();

    create_tmp_test_folder();

    machine.create_database(&database1);
    machine.create_table(&database1, &table1);
    assert!(machine.table_exists(&database1, &table1));

    destroy_tmp_test_folder();
}

#[test]
pub fn test_if_table_exists_is_false() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let mut machine = Machine::new();

    create_tmp_test_folder();

    machine.create_database(&database1);
    assert_eq!(machine.table_exists(&database1, &table1), false);

    destroy_tmp_test_folder();
}

#[test]
pub fn test_create_database_metadata_file_database1() {
    let database1 = String::from("database1");
    let mut machine = Machine::new();

    create_tmp_test_folder();

    machine.create_database(&database1);

    let metadata_foldername = format!("{}/database1/", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_create_table_metadata_file() {
    let database1 = String::from("database1");
    let mut machine = Machine::new();

    create_tmp_test_folder();

    machine.create_database(&database1);
    machine.create_table(&database1, &String::from("table1"));

    let table_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&table_filename).exists());

    destroy_tmp_test_folder();
}

#[test]
pub fn test_write_data_metadata_file() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");

    let mut machine = Machine::new();

    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.append(&mut 1u64.to_le_bytes().to_vec());
    buffer.append(&mut 1u64.to_le_bytes().to_vec());
    buffer.push(CellType::String as u8);
    buffer.append(&mut (bytes_array.len() as u16).to_le_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut cell = Cell::new();
    cell.string_to_bin(&data);

    let mut tuple = Tuple::new();
    tuple.push_string(&data);

    let mut tuples: Vec<Tuple> = Vec::new();

    tuples.push(tuple);

    create_tmp_test_folder();

    machine.create_database(&database1);
    machine.create_table(&database1, &table1);
    machine.insert_tuples(&database1, &table1, &mut tuples);


    let metadata_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&metadata_filename).exists());

    let actual_content = read_from_file(&metadata_filename).expect("Failed to read from file");
    assert_eq!(actual_content, raw_buffer, "File content does not match expected content");

    destroy_tmp_test_folder();
}
