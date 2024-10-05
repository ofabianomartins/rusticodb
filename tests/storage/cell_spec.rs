use rusticodb::storage::cell::Cell;
use rusticodb::storage::cell::CellType;

#[test]
pub fn test_cell_insert_string_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(CellType::String as u8);
    buffer.append(&mut (bytes_array.len() as u16).to_le_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut cell = Cell::new();

    cell.string_to_bin(&data);

    assert_eq!(cell.data, buffer);
}

#[test]
pub fn test_cell_insert_text_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(CellType::Text as u8);
    buffer.append(&mut (bytes_array.len() as u32).to_le_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut cell = Cell::new();

    cell.text_to_bin(&data);

    assert_eq!(cell.data, buffer);
}

#[test]
pub fn test_cell_insert_true_boolean_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Boolean as u8);
    buffer.push(1u8);

    let mut cell = Cell::new();

    cell.boolean_to_bin(true);

    assert_eq!(cell.data, buffer);
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

#[test]
pub fn test_cell_get_u8_to_string() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(CellType::String as u8);
    buffer.append(&mut (bytes_array.len() as u16).to_le_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut cell = Cell::new();
    cell.load(buffer);

    match cell.bin_to_string() {
        Ok(format_data) => {
            assert_eq!(format_data, data);
        },
        _ => {
            

        }
    }
}

#[test]
pub fn test_cell_get_u8_to_text() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(CellType::Text as u8);
    buffer.append(&mut (bytes_array.len() as u32).to_le_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut cell = Cell::new();
    cell.load(buffer);

    match cell.bin_to_text() {
        Ok(format_data) => {
            assert_eq!(format_data, data);
        },
        _ => {

        }
    }
}

#[test]
pub fn test_cell_get_u8_to_true_boolean() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Boolean as u8);
    buffer.push(1u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    match cell.bin_to_boolean() {
        Ok(format_data) => {
            assert_eq!(format_data, true);
        },
        _ => {

        }
    }
}

#[test]
pub fn test_cell_get_u8_to_false_boolean() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Boolean as u8);
    buffer.push(0u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    match cell.bin_to_boolean() {
        Ok(format_data) => {
            assert_eq!(format_data, false);
        },
        _ => {

        }
    }
}
