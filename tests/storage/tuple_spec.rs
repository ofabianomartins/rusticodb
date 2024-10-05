use rusticodb::storage::cell::{Cell, CellType};
use rusticodb::storage::tuple::Tuple;
use rusticodb::storage::os_interface::BLOCK_SIZE;

#[test]
pub fn test_tuple_push_string() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.append(&mut 1u64.to_le_bytes().to_vec());
    buffer.push(CellType::String as u8);
    buffer.append(&mut (bytes_array.len() as u16).to_le_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = Tuple::new();
    tuple.push_string(&data);

    assert_eq!(tuple.to_raw_data(), raw_buffer);
}

#[test]
pub fn test_tuple_push_text() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.append(&mut 1u64.to_le_bytes().to_vec());
    buffer.push(CellType::Text as u8);
    buffer.append(&mut (bytes_array.len() as u32).to_le_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = Tuple::new();
    tuple.push_text(&data);

    assert_eq!(tuple.to_raw_data(), raw_buffer);
}

#[test]
pub fn test_push_boolean_true() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u64.to_le_bytes().to_vec());
    buffer.push(CellType::Boolean as u8);
    buffer.push(1u8);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = Tuple::new();
    tuple.push_boolean(true);

    assert_eq!(tuple.to_raw_data(), raw_buffer);
}

#[test]
pub fn test_cell_insert_false_boolean_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Boolean as u8);
    buffer.push(0u8);

    let mut cell = Cell::new();

    cell.boolean_to_bin(false);

    assert_eq!(cell.data, buffer);
}

#[test]
pub fn test_cell_insert_unsigned_tinyint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::UnsignedTinyint as u8);
    buffer.push(50u8);

    let mut cell = Cell::new();

    cell.unsigned_tinyint_to_bin(50u8);

    assert_eq!(cell.data, buffer);
}

#[test]
pub fn test_cell_insert_unsigned_smallint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::UnsignedSmallint as u8);
    buffer.append(&mut 50u16.to_le_bytes().to_vec());

    let mut cell = Cell::new();

    cell.unsigned_smallint_to_bin(50u16);

    assert_eq!(cell.data, buffer);
}


#[test]
pub fn test_cell_insert_unsigned_int_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::UnsignedInt as u8);
    buffer.append(&mut 50u32.to_le_bytes().to_vec());

    let mut cell = Cell::new();

    cell.unsigned_int_to_bin(50u32);

    assert_eq!(cell.data, buffer);
}

#[test]
pub fn test_cell_insert_unsigned_bigint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::UnsignedBigint as u8);
    buffer.append(&mut 50u64.to_le_bytes().to_vec());

    let mut cell = Cell::new();

    cell.unsigned_bigint_to_bin(50u64);

    assert_eq!(cell.data, buffer);
}

#[test]
pub fn test_cell_insert_signed_tinyint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::SignedTinyint as u8);
    buffer.append(&mut 50i8.to_le_bytes().to_vec());

    let mut cell = Cell::new();

    cell.signed_tinyint_to_bin(50i8);

    assert_eq!(cell.data, buffer);
}

#[test]
pub fn test_cell_insert_signed_smallint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::SignedSmallint as u8);
    buffer.append(&mut 50i16.to_le_bytes().to_vec());

    let mut cell = Cell::new();

    cell.signed_smallint_to_bin(50i16);

    assert_eq!(cell.data, buffer);
}


#[test]
pub fn test_cell_insert_signed_int_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::SignedInt as u8);
    buffer.append(&mut 50i32.to_le_bytes().to_vec());

    let mut cell = Cell::new();

    cell.signed_int_to_bin(50i32);

    assert_eq!(cell.data, buffer);
}

#[test]
pub fn test_cell_insert_signed_bigint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::SignedBigint as u8);
    buffer.append(&mut 50i64.to_le_bytes().to_vec());

    let mut cell = Cell::new();

    cell.signed_bigint_to_bin(50i64);

    assert_eq!(cell.data, buffer);
}

