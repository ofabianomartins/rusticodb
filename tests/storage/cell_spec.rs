use rusticodb::storage::cell::Cell;
use rusticodb::storage::cell::CellType;
use rusticodb::storage::cell::ParserError;

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
        _ => { }
    }
}

#[test]
pub fn test_cell_get_u8_to_string_with_error() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(CellType::Text as u8);
    buffer.append(&mut (bytes_array.len() as u16).to_le_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_string(), Err(ParserError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_string_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_string(), Err(ParserError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_string_with_length_error2() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(CellType::String as u8);
    buffer.append(&mut ((bytes_array.len() + 5) as u16).to_le_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_string(), Err(ParserError::WrongLength)));
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
        _ => { }
    }
}

#[test]
pub fn test_cell_get_u8_to_text_with_error() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(CellType::String as u8);
    buffer.append(&mut (bytes_array.len() as u16).to_le_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_text(),Err(ParserError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_text_with_lentgh_error() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    buffer.push(CellType::String as u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_text(),Err(ParserError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_text_with_length_error2() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(CellType::Text as u8);
    buffer.append(&mut ((bytes_array.len() + 5) as u32).to_le_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_text(),Err(ParserError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_true_boolean() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Boolean as u8);
    buffer.push(1u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_boolean(), Ok(true)));
}

#[test]
pub fn test_cell_get_u8_to_false_boolean() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Boolean as u8);
    buffer.push(0u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_boolean(), Ok(false)));
}

#[test]
pub fn test_cell_get_u8_to_false_boolean_with_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);
    buffer.push(0u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_boolean(), Err(ParserError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_false_boolean_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_boolean(), Err(ParserError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_tinyint() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::UnsignedTinyint as u8);
    buffer.push(50u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_unsigned_tinyint(), Ok(50u8)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_tinyint_with_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);
    buffer.push(50u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_unsigned_tinyint(), Err(ParserError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_tinyint_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_unsigned_tinyint(), Err(ParserError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_smallint() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::UnsignedSmallint as u8);
    buffer.append(&mut 350u16.to_le_bytes().to_vec());

    let mut cell = Cell::new();
    cell.load(buffer);


    match cell.bin_to_unsigned_smallint() {
        Ok(format_data) => {
            assert_eq!(format_data, 350u16);
        },
        _ => { }
    }
}

#[test]
pub fn test_cell_get_u8_to_unsigned_smallint_with_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);
    buffer.append(&mut 50u16.to_le_bytes().to_vec());

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_unsigned_smallint(), Err(ParserError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_smallint_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_unsigned_smallint(), Err(ParserError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_int() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::UnsignedInt as u8);
    buffer.append(&mut 100350u32.to_le_bytes().to_vec());

    let mut cell = Cell::new();
    cell.load(buffer);


    match cell.bin_to_unsigned_int() {
        Ok(format_data) => {
            assert_eq!(format_data, 100350u32);
        },
        _ => { }
    }
}

#[test]
pub fn test_cell_get_u8_to_unsigned_int_with_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);
    buffer.append(&mut 50u32.to_le_bytes().to_vec());

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_unsigned_int(), Err(ParserError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_int_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_unsigned_int(), Err(ParserError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_bigint() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::UnsignedBigint as u8);
    buffer.append(&mut 14294967295u64.to_le_bytes().to_vec());

    let mut cell = Cell::new();
    cell.load(buffer);


    match cell.bin_to_unsigned_bigint() {
        Ok(format_data) => {
            assert_eq!(format_data, 14294967295u64);
        },
        _ => { }
    }
}

#[test]
pub fn test_cell_get_u8_to_unsigned_bigint_with_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);
    buffer.append(&mut 14294967295u64.to_le_bytes().to_vec());

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_unsigned_bigint(), Err(ParserError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_bigint_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_unsigned_bigint(), Err(ParserError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_signed_tinyint() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::SignedTinyint as u8);
    buffer.push(-123i8 as u8);

    let mut cell = Cell::new();
    cell.load(buffer);


    match cell.bin_to_signed_tinyint() {
        Ok(format_data) => {
            assert_eq!(format_data, -123i8);
        },
        _ => { }
    }
}

#[test]
pub fn test_cell_get_u8_to_signed_tinyint_with_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);
    buffer.push(-123i8 as u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_signed_tinyint(), Err(ParserError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_signed_tinyint_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_signed_tinyint(), Err(ParserError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_signed_smallint() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::SignedSmallint as u8);
    buffer.append(&mut (-31122i16).to_le_bytes().to_vec());

    let mut cell = Cell::new();
    cell.load(buffer);


    match cell.bin_to_signed_smallint() {
        Ok(format_data) => {
            assert_eq!(format_data, -31123i16);
        },
        _ => { }
    }
}

#[test]
pub fn test_cell_get_u8_to_signed_smallint_with_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);
    buffer.append(&mut 50i16.to_le_bytes().to_vec());

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_signed_smallint(), Err(ParserError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_signed_smallint_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_signed_smallint(), Err(ParserError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_signed_int() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::SignedInt as u8);
    buffer.append(&mut 100350i32.to_le_bytes().to_vec());

    let mut cell = Cell::new();
    cell.load(buffer);

    match cell.bin_to_signed_int() {
        Ok(format_data) => {
            assert_eq!(format_data, 100350i32);
        },
        _ => { }
    }
}

#[test]
pub fn test_cell_get_u8_to_signed_int_with_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);
    buffer.append(&mut 50i32.to_le_bytes().to_vec());

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_signed_int(), Err(ParserError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_signed_int_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_signed_int(), Err(ParserError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_signed_bigint() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::SignedBigint as u8);
    buffer.append(&mut 14294967295i64.to_le_bytes().to_vec());

    let mut cell = Cell::new();
    cell.load(buffer);

    match cell.bin_to_signed_bigint() {
        Ok(format_data) => {
            assert_eq!(format_data, 14294967295i64);
        },
        _ => { }
    }
}

#[test]
pub fn test_cell_get_u8_to_signed_bigint_with_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);
    buffer.append(&mut 14294967295i64.to_le_bytes().to_vec());

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_signed_bigint(), Err(ParserError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_signed_bigint_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::String as u8);

    let mut cell = Cell::new();
    cell.load(buffer);

    assert!(matches!(cell.bin_to_signed_bigint(), Err(ParserError::WrongLength)));
}
