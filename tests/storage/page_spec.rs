use rusticodb::storage::Tuple;
use rusticodb::storage::page_new;
use rusticodb::storage::page_amount_left;
use rusticodb::storage::page_insert_tuples;
use rusticodb::storage::page_read_tuples;
use rusticodb::storage::page_serialize;
use rusticodb::storage::page_deserialize;
use rusticodb::storage::Data;
use rusticodb::storage::BLOCK_SIZE;
use rusticodb::storage::tuple_new;

#[test]
pub fn test_a_new_page() {
    let page = page_new();

    assert_eq!(page_serialize(&page), [0u8; BLOCK_SIZE]);
}

#[test]
pub fn test_a_empty_page_amount_left() {
    let page = page_new();

    assert_eq!(page_amount_left(&page) as usize, BLOCK_SIZE - 2);
}

#[test]
pub fn test_insert_one_tuple_and_with_page_amount_left() {
    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedTinyint(2u8));
    tuples.push(tuple);

    let mut page = page_new();
    page_insert_tuples(&mut page, &mut tuples);

    assert_eq!(page_amount_left(&page) as usize, BLOCK_SIZE - (2*2 + 3));
}

#[test]
pub fn test_insert_one_tuple_and_with_two_cells_page_amount_left() {
    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedTinyint(2u8));
    tuple.push(Data::UnsignedTinyint(2u8));
    tuples.push(tuple);

    let mut page = page_new();
    page_insert_tuples(&mut page, &mut tuples);

    assert_eq!(page_amount_left(&page) as usize, BLOCK_SIZE - (2*2 + 5));
}

#[test]
pub fn test_insert_two_tuples_and_page_amount_left() {
    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedTinyint(2u8));
    tuples.push(tuple);

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedTinyint(2u8));
    tuples.push(tuple);

    let mut page = page_new();
    page_insert_tuples(&mut page, &mut tuples);

    assert_eq!(page_amount_left(&page) as usize, BLOCK_SIZE - (2*3 + 3 + 3) );
}

#[test]
pub fn test_insert_tuple_with_tinyint() {
    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedTinyint(2u8));
    tuples.push(tuple);

    let mut page = page_new();
    page_insert_tuples(&mut page, &mut tuples);

    let mut buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    buffer[1] = 1u8;
    buffer[2] = 15u8;
    buffer[3] = 253u8;
    buffer[BLOCK_SIZE-3] = 1u8;
    buffer[BLOCK_SIZE-2] = 4;
    buffer[BLOCK_SIZE-1] = 2u8;

    assert_eq!(page_serialize(&page), buffer);
}

#[test]
pub fn test_insert_two_tuples_with_tinyint() {
    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    raw_buffer[1] = 2;
    raw_buffer[2] = 15;
    raw_buffer[3] = 253;
    raw_buffer[4] = 15;
    raw_buffer[5] = 250;

    let tuple_position = BLOCK_SIZE - 2*3;
    raw_buffer[tuple_position] = 1;
    raw_buffer[tuple_position + 1] = 4;
    raw_buffer[tuple_position + 2] = 1;

    let tuple_position = BLOCK_SIZE - 3;
    raw_buffer[tuple_position + 0] = 1;
    raw_buffer[tuple_position + 1] = 4;
    raw_buffer[tuple_position + 2] = 1;

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedTinyint(1u8));
    tuples.push(tuple);

    let mut tuples2: Vec<Tuple> = Vec::new();
    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedTinyint(1u8));
    tuples.push(tuple);

    let mut page = page_new();
    page_insert_tuples(&mut page, &mut tuples);
    page_insert_tuples(&mut page, &mut tuples2);

    assert_eq!(page_serialize(&page), raw_buffer);
}

#[test]
pub fn test_insert_tuple_with_string() {
    let data: String = String::from("simple_string");

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    raw_buffer[1] = 1;
    raw_buffer[2] = 15;
    raw_buffer[3] = 239;

    let tuple_position = BLOCK_SIZE - (data.len() + 5);
    raw_buffer[tuple_position + 1] = 1;
    raw_buffer[tuple_position + 2] = 12;
    raw_buffer[tuple_position + 3] = 0;
    raw_buffer[tuple_position + 4] = 13;
    for (idx, elem) in data.clone().into_bytes().iter().enumerate() {
        raw_buffer[tuple_position + 5 + idx] = *elem;
    }

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple = tuple_new();
    tuple.push(Data::Varchar(data.clone()));
    tuples.push(tuple);

    let mut page = page_new();

    page_insert_tuples(&mut page, &mut tuples);

    assert_eq!(page_serialize(&page), raw_buffer);
}

#[test]
pub fn test_insert_two_tuples_with_string() {
    let data: String = String::from("simple_string");

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    raw_buffer[1] = 2;
    raw_buffer[2] = 15;
    raw_buffer[3] = 239;
    raw_buffer[4] = 15;
    raw_buffer[5] = 222;

    let tuple_position = BLOCK_SIZE - 2*(data.len() + 4);
    raw_buffer[tuple_position] = 1;
    raw_buffer[tuple_position + 1] = 12;
    raw_buffer[tuple_position + 2] = 0;
    raw_buffer[tuple_position + 3] = 13;
    for (idx, elem) in data.clone().into_bytes().iter().enumerate() {
        raw_buffer[tuple_position + 4 + idx] = *elem;
    }

    let tuple_position = BLOCK_SIZE - (data.len() + 4);
    raw_buffer[tuple_position + 0] = 1;
    raw_buffer[tuple_position + 1] = 12;
    raw_buffer[tuple_position + 2] = 0;
    raw_buffer[tuple_position + 3] = 13;
    for (idx, elem) in data.clone().into_bytes().iter().enumerate() {
        raw_buffer[tuple_position + 4 + idx] = *elem;
    }

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple = tuple_new();
    tuple.push(Data::Varchar(data.clone()));
    tuples.push(tuple);

    let mut tuples2: Vec<Tuple> = Vec::new();
    let mut tuple = tuple_new();
    tuple.push(Data::Varchar(data.clone()));
    tuples.push(tuple);

    let mut page = page_new();

    page_insert_tuples(&mut page, &mut tuples);
    page_insert_tuples(&mut page, &mut tuples2);

    assert_eq!(page_serialize(&page), raw_buffer);
}

