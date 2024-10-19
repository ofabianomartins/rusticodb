use rusticodb::storage::cell::CellType;
use rusticodb::storage::tuple::Tuple;
use rusticodb::storage::page::Page;
use rusticodb::storage::os_interface::BLOCK_SIZE;

#[test]
pub fn test2_insert_two_tuples_on_pager_and_read_both() {
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

    let mut page = Page::new(0);

    page.insert_tuples(&mut tuples);
    page.insert_tuples(&mut tuples2);

    let tuples = page.read_tuples();

    assert_eq!(page.data, raw_buffer);
    assert_eq!(tuples.len(), 2);
}
