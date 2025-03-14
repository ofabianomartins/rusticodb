use rusticodb::storage::Tuple;
use rusticodb::storage::page_new;
use rusticodb::storage::page_insert_tuples;
use rusticodb::storage::page_read_tuples;
use rusticodb::storage::CellType;
use rusticodb::storage::BLOCK_SIZE;
use rusticodb::storage::tuple_cell_count;
use rusticodb::storage::tuple_push_unsigned_tinyint;
use rusticodb::storage::tuple_push_varchar;
use rusticodb::storage::tuple_new;

#[test]
pub fn test2_insert_two_tuples_on_pager_and_read_both() {
    let data: String = String::from("simple_string");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple = tuple_new();
    tuple_push_varchar(&mut tuple, &data);
    tuples.push(tuple);

    let mut tuples2: Vec<Tuple> = Vec::new();
    let mut tuple = tuple_new();
    tuple_push_varchar(&mut tuple, &data);
    tuples.push(tuple);

    let mut page = page_new(0);
    page_insert_tuples(&mut page, &mut tuples);
    page_insert_tuples(&mut page, &mut tuples2);

    let tuples = page_read_tuples(&page);

    assert_eq!(tuples.len(), 2);
    assert_eq!(tuple_cell_count(&tuples[0]), 1);
    assert_eq!(tuple_cell_count(&tuples[1]), 1);
}

#[test]
pub fn test_insert_one_tuple_in_the_end_of_page() {
    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple = tuple_new();
    tuple_push_unsigned_tinyint(&mut tuple, 2u8);
    tuples.push(tuple);

    let mut page = page_new(0);
    page_insert_tuples(&mut page, &mut tuples);

    let tuples = page_read_tuples(&page);

    let mut buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    buffer[1] = 1u8;
    buffer[2] = 15u8;
    buffer[3] = 251u8;
    buffer[BLOCK_SIZE-4] = 1u8;
    buffer[BLOCK_SIZE-3] = 6u8;
    buffer[BLOCK_SIZE-2] = CellType::UnsignedTinyint as u8;
    buffer[BLOCK_SIZE-1] = 2u8;

    assert_eq!(tuples.len(), 1);
    assert_eq!(tuple_cell_count(&tuples[0]), 1);
    assert_eq!(page, buffer);
}
