use rusticodb::storage::Cell;
use rusticodb::storage::CellType;
use rusticodb::utils::ExecutionError;

#[test]
pub fn test_cell_get_u8_to_string() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(CellType::Varchar as u8);
    buffer.append(&mut (bytes_array.len() as u16).to_be_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let cell = Cell::load_cell(buffer);

    match cell.bin_to_varchar() {
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
    buffer.append(&mut (bytes_array.len() as u16).to_be_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let cell = Cell::load_cell(buffer);

    assert!(matches!(cell.bin_to_varchar(), Err(ExecutionError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_string_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(CellType::Varchar as u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_varchar(), Err(ExecutionError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_string_with_length_error2() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(CellType::Varchar as u8);
    buffer.append(&mut ((bytes_array.len() + 5) as u16).to_be_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_varchar(), Err(ExecutionError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_text() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(CellType::Text as u8);
    buffer.append(&mut (bytes_array.len() as u32).to_be_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let cell = Cell::load_cell(buffer);
    
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

    buffer.push(CellType::Varchar as u8);
    buffer.append(&mut (bytes_array.len() as u16).to_be_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_text(),Err(ExecutionError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_text_with_lentgh_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Varchar as u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_text(),Err(ExecutionError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_text_with_length_error2() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.push(CellType::Text as u8);
    buffer.append(&mut ((bytes_array.len() + 5) as u32).to_be_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_text(),Err(ExecutionError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_true_boolean() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Boolean as u8);
    buffer.push(1u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_boolean(), Ok(true)));
}

#[test]
pub fn test_cell_get_u8_to_false_boolean() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Boolean as u8);
    buffer.push(0u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_boolean(), Ok(false)));
}

#[test]
pub fn test_cell_get_u8_to_false_boolean_with_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Varchar as u8);
    buffer.push(0u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_boolean(), Err(ExecutionError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_false_boolean_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Varchar as u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_boolean(), Err(ExecutionError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_tinyint() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::UnsignedTinyint as u8);
    buffer.push(50u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_unsigned_tinyint(), Ok(50u8)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_tinyint_with_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Varchar as u8);
    buffer.push(50u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_unsigned_tinyint(), Err(ExecutionError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_tinyint_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Varchar as u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_unsigned_tinyint(), Err(ExecutionError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_smallint() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::UnsignedSmallint as u8);
    buffer.append(&mut 350u16.to_be_bytes().to_vec());

    let cell = Cell::load_cell(buffer);
    

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

    buffer.push(CellType::Varchar as u8);
    buffer.append(&mut 50u16.to_be_bytes().to_vec());

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_unsigned_smallint(), Err(ExecutionError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_smallint_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Varchar as u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_unsigned_smallint(), Err(ExecutionError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_int() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::UnsignedInt as u8);
    buffer.append(&mut 100350u32.to_be_bytes().to_vec());

    let cell = Cell::load_cell(buffer);
    

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

    buffer.push(CellType::Varchar as u8);
    buffer.append(&mut 50u32.to_be_bytes().to_vec());

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_unsigned_int(), Err(ExecutionError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_int_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(CellType::Varchar as u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_unsigned_int(), Err(ExecutionError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_bigint() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(CellType::UnsignedBigint as u8);
    buffer.append(&mut 14294967295u64.to_be_bytes().to_vec());

    let cell = Cell::load_cell(buffer);
    
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
    buffer.push(CellType::Varchar as u8);
    buffer.append(&mut 14294967295u64.to_be_bytes().to_vec());

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_unsigned_bigint(), Err(ExecutionError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_unsigned_bigint_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(CellType::Varchar as u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_unsigned_bigint(), Err(ExecutionError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_signed_tinyint() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(CellType::SignedTinyint as u8);
    buffer.push(-123i8 as u8);

    let cell = Cell::load_cell(buffer);
    
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
    buffer.push(CellType::Varchar as u8);
    buffer.push(-123i8 as u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_signed_tinyint(), Err(ExecutionError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_signed_tinyint_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(CellType::Varchar as u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_signed_tinyint(), Err(ExecutionError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_signed_smallint() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(CellType::SignedSmallint as u8);
    buffer.append(&mut (-31122i16).to_be_bytes().to_vec());

    let cell = Cell::load_cell(buffer);
    
    match cell.bin_to_signed_smallint() {
        Ok(format_data) => {
            assert_eq!(format_data, -31122i16);
        },
        _ => { }
    }
}

#[test]
pub fn test_cell_get_u8_to_signed_smallint_with_error() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(CellType::Varchar as u8);
    buffer.append(&mut 50i16.to_be_bytes().to_vec());

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_signed_smallint(), Err(ExecutionError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_signed_smallint_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(CellType::Varchar as u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_signed_smallint(), Err(ExecutionError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_signed_int() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(CellType::SignedInt as u8);
    buffer.append(&mut 100350i32.to_be_bytes().to_vec());

    let cell = Cell::load_cell(buffer);
    
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
    buffer.push(CellType::Varchar as u8);
    buffer.append(&mut 50i32.to_be_bytes().to_vec());

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_signed_int(), Err(ExecutionError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_signed_int_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(CellType::Varchar as u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_signed_int(), Err(ExecutionError::WrongLength)));
}

#[test]
pub fn test_cell_get_u8_to_signed_bigint() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(CellType::SignedBigint as u8);
    buffer.append(&mut 14294967295i64.to_be_bytes().to_vec());

    let cell = Cell::load_cell(buffer);
    
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
    buffer.push(CellType::Varchar as u8);
    buffer.append(&mut 14294967295i64.to_be_bytes().to_vec());

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_signed_bigint(), Err(ExecutionError::WrongFormat)));
}

#[test]
pub fn test_cell_get_u8_to_signed_bigint_with_length_error() {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.push(CellType::Varchar as u8);

    let cell = Cell::load_cell(buffer);
    
    assert!(matches!(cell.bin_to_signed_bigint(), Err(ExecutionError::WrongLength)));
}
