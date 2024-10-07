use std::path::Path;

use rusticodb::config::Config;
use rusticodb::machine::machine::Machine;
use rusticodb::storage::cell::CellType;
use rusticodb::storage::tuple::Tuple;
use rusticodb::storage::pager::Pager;
use rusticodb::storage::pager::Page;
use rusticodb::storage::os_interface::BLOCK_SIZE;

use crate::test_utils::create_tmp_test_folder;
use crate::test_utils::destroy_tmp_test_folder;
use crate::test_utils::read_from_file;

#[test]
pub fn test_set_tuple_count_bigger_than_255() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(1);
    buffer.push(44);
    buffer.push(0);
    buffer.push(4);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut page = Page::new(0);

    page.set_tuple_count(300u16);

    assert_eq!(page.data, raw_buffer);
    assert_eq!(page.tuple_count(), 300u16);
}

#[test]
pub fn test_a_empty_page() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(0);
    buffer.push(0);
    buffer.push(0);
    buffer.push(4);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let page = Page::new(0);

    assert_eq!(page.data, raw_buffer);
}

#[test]
pub fn test_insert_tuples_on_pager() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");

    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(0);
    buffer.push(1);
    buffer.push(0);
    buffer.push(22);
    buffer.push(0);
    buffer.push(1);
    buffer.push(CellType::String as u8);
    buffer.push(0);
    buffer.push(13);
    buffer.append(&mut bytes_array);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple = Tuple::new();
    tuple.push_string(&data);
    tuples.push(tuple);

    let mut pager = Pager::new();

    pager.insert_tuples(&database1, &table1, &mut tuples);

    let page_key = format!("{}/{}/{}.db", Config::data_folder(), database1, table1);
    let page: &Page = pager.pages.get(&page_key).unwrap();

    assert_eq!(page.data, raw_buffer);
}

#[test]
pub fn test2_insert_tuples_on_pager_and_add_more_tuples() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");

    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(0);
    buffer.push(2);
    buffer.push(0);
    buffer.push(40);
    buffer.push(0);
    buffer.push(1);
    buffer.push(CellType::String as u8);
    buffer.push(0);
    buffer.push(13);
    buffer.append(&mut bytes_array);

    let mut bytes_array = data.clone().into_bytes();
    buffer.push(0);
    buffer.push(1);
    buffer.push(CellType::String as u8);
    buffer.push(0);
    buffer.push(13);
    buffer.append(&mut bytes_array);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple = Tuple::new();
    tuple.push_string(&data);
    tuples.push(tuple);

    let mut tuples2: Vec<Tuple> = Vec::new();
    let mut tuple = Tuple::new();
    tuple.push_string(&data);
    tuples.push(tuple);

    let mut pager = Pager::new();

    pager.insert_tuples(&database1, &table1, &mut tuples);
    pager.insert_tuples(&database1, &table1, &mut tuples2);

    let page_key = format!("{}/{}/{}.db", Config::data_folder(), database1, table1);
    let page: &Page = pager.pages.get(&page_key).unwrap();

    assert_eq!(page.data, raw_buffer);
}

#[test]
pub fn test_write_data_metadata_file() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");

    let data = [2u8; BLOCK_SIZE];

    let mut machine = Machine::new();
    let mut pager = Pager::new();

    create_tmp_test_folder();

    machine.create_database(&database1);
    machine.create_table(&database1, &table1);
    pager.write_data(&database1, &table1, 0u64, &data);

    let metadata_filename = format!("{}/database1/table1.db", Config::data_folder());
    assert!(Path::new(&metadata_filename).exists());

    let rows_filename = format!("{}/{}/{}.db", Config::data_folder(), &database1, &table1);
     // Read the content back from the file
    let actual_content = read_from_file(&rows_filename).expect("Failed to read from file");
    assert_eq!(actual_content, data, "File content does not match expected content");

    destroy_tmp_test_folder();
}

#[test]
pub fn test_read_data_metadata_file() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");

    let data = [2u8; BLOCK_SIZE];

    let mut machine = Machine::new();
    let mut pager = Pager::new();

    create_tmp_test_folder();

    machine.create_database(&database1);
    machine.create_table(&database1, &table1);
    pager.write_data(&database1, &table1, 0u64, &data);

    let actual_content = pager.read_data(&database1, &table1, 0u64);
    assert_eq!(actual_content, data, "File content does not match expected content");

    destroy_tmp_test_folder();
}
