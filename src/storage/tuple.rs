use crate::storage::Cell;
use crate::storage::CellType;
use crate::storage::BLOCK_SIZE;

use crate::utils::ExecutionError;

pub type Tuple = Vec<u8>;

pub fn tuple_new() -> Tuple {
    let mut data: Vec<u8> = Vec::new();
    data.push(0);
    data.push(0);
    data.push(0);
    data.push(4);
    data
}

pub fn tuple_append_cell(tuple: &mut Tuple, mut data: Vec<u8>) {
    tuple_set_cell_count(tuple, tuple_cell_count(tuple) + 1);
    tuple_set_data_size(tuple, tuple_data_size(tuple) + (data.len() as u16));
    tuple.append(&mut data);
}

pub fn tuple_get_cell(tuple: &Tuple, position: u16) -> Vec<u8> {
    let cell_count = tuple_cell_count(tuple);

    if position >= cell_count {
        return Vec::new();
    }

    let mut cell_index = 0;
    let mut position_index: usize = 4;
    let mut cell_size: u32;

    loop {
        if position_index >= tuple.len() {
            return Vec::new();
        }

        if tuple[position_index as usize] == (CellType::Varchar as u8) {
            let byte_array: [u8; 2] = [tuple[position_index + 1], tuple[position_index + 2]];
            cell_size = (u16::from_be_bytes(byte_array) as u32) + 3u32; // or use `from_be_bytes` for big-endian
        } else if tuple[position_index as usize] == (CellType::Text as u8) {
            let byte_array: [u8; 4] = [
                tuple[position_index + 1],
                tuple[position_index + 2],
                tuple[position_index + 3],
                tuple[position_index + 4]
            ];
            cell_size = (u32::from_be_bytes(byte_array) as u32) + 5u32; // or use `from_be_bytes` for big-endian
        } else if tuple[position_index as usize] == (CellType::Text as u8) {
            let byte_array: [u8; 4] = [
                tuple[position_index + 1], tuple[position_index + 2],
                tuple[position_index + 3], tuple[position_index + 4]
            ];
            cell_size = u32::from_be_bytes(byte_array) + 6u32; // or use `from_be_bytes` for big-endian
        } else {
            cell_size = Cell::count_data_size(tuple[position_index as usize]);
        }

        if cell_index >= cell_count || cell_index == position {
            break;
        }

        cell_index += 1;
        position_index += cell_size as usize;
    }
    let mut buffer_array: Vec<u8> = Vec::new();
    for n in position_index..(position_index + (cell_size as usize)) {
        buffer_array.push(tuple[n as usize]);
    }
    return buffer_array;
}

pub fn tuple_push_null(tuple: &mut Tuple) {
    tuple_append_cell(tuple, vec![CellType::Null as u8]);
}

pub fn tuple_push_varchar(tuple: &mut Tuple, raw_data: &String) {
    let mut bytes_array = raw_data.clone().into_bytes();

    let size = bytes_array.len() as u16;  

    let mut data = Vec::new();

    data.push(CellType::Varchar as u8);
    data.append(&mut size.to_be_bytes().to_vec());
    data.append(&mut bytes_array);
    tuple_append_cell(tuple, data);
}

pub fn tuple_push_text(tuple: &mut Tuple, raw_data: &String) {
    let mut bytes_array = raw_data.clone().into_bytes();

    let size = bytes_array.len() as u32;  

    let mut data = Vec::new();

    data.push(CellType::Text as u8);
    data.append(&mut size.to_be_bytes().to_vec());
    data.append(&mut bytes_array);
    tuple_append_cell(tuple, data);
}

pub fn tuple_push_boolean(tuple: &mut Tuple, value: bool) {
    tuple_append_cell(tuple, vec![CellType::Boolean as u8, value as u8]);
}

pub fn tuple_push_unsigned_tinyint(tuple: &mut Tuple, value: u8) {
    tuple_append_cell(tuple, vec![CellType::UnsignedTinyint as u8, value]);
}

pub fn tuple_push_unsigned_smallint(tuple: &mut Tuple, value: u16) {
    let mut data = Vec::new();
    data.push(CellType::UnsignedSmallint as u8);
    data.append(&mut value.to_be_bytes().to_vec());
    tuple_append_cell(tuple, data);
}

pub fn tuple_push_unsigned_int(tuple: &mut Tuple, value: u32) {
    let mut data = Vec::new();
    data.push(CellType::UnsignedInt as u8);
    data.append(&mut value.to_be_bytes().to_vec());
    tuple_append_cell(tuple, data);
}

pub fn tuple_push_unsigned_bigint(tuple: &mut Tuple, value: u64) {
    let mut data = Vec::new();
    data.push(CellType::UnsignedBigint as u8);
    data.append(&mut value.to_be_bytes().to_vec());
    tuple_append_cell(tuple, data);
}

pub fn tuple_push_signed_tinyint(tuple: &mut Tuple, value: i8) {
    let mut data = Vec::new();
    data.push(CellType::SignedTinyint as u8);
    data.append(&mut value.to_be_bytes().to_vec());
    tuple_append_cell(tuple, data);
}

pub fn tuple_push_signed_smallint(tuple: &mut Tuple, value: i16) {
    let mut data = Vec::new();
    data.push(CellType::SignedSmallint as u8);
    data.append(&mut value.to_be_bytes().to_vec());
    tuple_append_cell(tuple, data);
}

pub fn tuple_push_signed_int(tuple: &mut Tuple, value: i32) {
    let mut data = Vec::new();
    data.push(CellType::SignedInt as u8);
    data.append(&mut value.to_be_bytes().to_vec());
    tuple_append_cell(tuple, data);
}

pub fn tuple_push_signed_bigint(tuple: &mut Tuple, value: i64) {
    let mut data = Vec::new();
    data.push(CellType::SignedBigint as u8);
    data.append(&mut value.to_be_bytes().to_vec());
    tuple_append_cell(tuple, data);
}

pub fn tuple_get_vec_u8(tuple: &Tuple, position: u16) -> Result<Vec<u8>, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(Vec::new());
    }

    return Ok(tuple_get_cell(tuple, position));
}

pub fn tuple_get_varchar(tuple: &Tuple, position: u16) -> Result<String, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(String::from(""));
    }

    return bin_to_varchar(&tuple_get_cell(tuple, position));
}

pub fn tuple_get_text(tuple: &Tuple, position: u16) -> Result<String, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(String::from(""));
    }

    return bin_to_text(&tuple_get_cell(tuple, position));
}

pub fn tuple_get_boolean(tuple: &Tuple, position: u16) -> Result<bool, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(false);
    }

    return bin_to_boolean(&tuple_get_cell(tuple, position));
}

pub fn tuple_get_unsigned_tinyint(tuple: &Tuple, position: u16) -> Result<u8, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return bin_to_unsigned_tinyint(&tuple_get_cell(tuple, position));
}

pub fn tuple_get_unsigned_smallint(tuple: &Tuple, position: u16) -> Result<u16, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return bin_to_unsigned_smallint(&tuple_get_cell(tuple, position));
}

pub fn tuple_get_unsigned_int(tuple: &Tuple, position: u16) -> Result<u32, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return bin_to_unsigned_int(&tuple_get_cell(tuple, position));
}

pub fn tuple_get_unsigned_bigint(tuple: &Tuple, position: u16) -> Result<u64, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return bin_to_unsigned_bigint(&tuple_get_cell(tuple, position));
}

pub fn tuple_get_signed_tinyint(tuple: &Tuple, position: u16) -> Result<i8, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return bin_to_signed_tinyint(&tuple_get_cell(tuple, position));
}

pub fn tuple_get_signed_smallint(tuple: &Tuple, position: u16) -> Result<i16, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return bin_to_signed_smallint(&tuple_get_cell(tuple, position));
}

pub fn tuple_get_signed_int(tuple: &Tuple, position: u16) -> Result<i32, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return bin_to_signed_int(&tuple_get_cell(tuple, position));
}

pub fn tuple_get_signed_bigint(tuple: &Tuple, position: u16) -> Result<i64, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return bin_to_signed_bigint(&tuple_get_cell(tuple, position));
}

pub fn tuple_set_cell_count(tuple: &mut Tuple, new_cell_count: u16) {
    if new_cell_count > 255 {
        tuple[0] = (new_cell_count >> 8) as u8;
    }
    tuple[1] = (new_cell_count % 256) as u8;
}

pub fn tuple_cell_count(tuple: &Tuple) -> u16 {
    if tuple.len() == 0 {
        return 0u16;
    }
    let byte_array: [u8; 2] = [tuple[0], tuple[1]];
    return u16::from_be_bytes(byte_array); // or use `from_be_bytes` for big-endian
}

pub fn tuple_set_data_size(tuple: &mut Tuple, new_data_size: u16) {
    if new_data_size > 255 {
        tuple[2] = (new_data_size >> 8) as u8;
    }
    tuple[3] = (new_data_size % 256) as u8;
}

pub fn tuple_data_size(tuple: &Tuple) -> u16 {
    if tuple.len() == 0 {
        return 0u16;
    }
    let byte_array: [u8; 2] = [tuple[2], tuple[3]];
    return u16::from_be_bytes(byte_array); // or use `from_be_bytes` for big-endian
}

pub fn tuple_to_raw_data(tuple: &Tuple) -> [u8; BLOCK_SIZE] {
    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];

    for (idx, elem) in &mut tuple.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }
    return raw_buffer;
}

pub fn tuple_display(tuple: &Tuple) {
    let cell_count = tuple_cell_count(tuple);
    let data_size = tuple_data_size(tuple);
    let mut cell_index = 0;

    print!("Tuple [{}, {}, (", cell_count, data_size);

    while cell_index < cell_count {
        print!("{:?}", tuple_get_cell(tuple, cell_index));

        if cell_index != cell_count - 1 {
          print!(",");
        }

        cell_index += 1;
    }
    print!(")]")
}

////////////////////////////////////////////////////////////////////////////////

pub fn is_true(data: &Vec<u8>) -> bool {
    data[0] == (CellType::Boolean as u8) && data[1] == 1
}

pub fn bin_to_varchar(data: &Vec<u8>) -> Result<String, ExecutionError> {
    if data.len() <= 1 {
        return Err(ExecutionError::WrongLength)
    } 

    if data[0] != (CellType::Varchar as u8) {
        return Err(ExecutionError::WrongFormat)
    } 

    let byte_array: [u8; 2] = [data[1], data[2]];
    let string_size = u16::from_be_bytes(byte_array);

    if data.len() != ((string_size + 3) as usize) {
        return Err(ExecutionError::WrongLength)
    } 

    let mut bytes: Vec<u8> = Vec::new();
    let mut index: usize = 3;

    while index < data.len() {
        bytes.push(*data.get(index).unwrap());
        index += 1;
    }

    match String::from_utf8(bytes) {
        Ok(new_data) => Ok(new_data),
        Err(_error) => Err(ExecutionError::StringParseFailed)
    }
}

pub fn bin_to_text(data: &Vec<u8>) -> Result<String, ExecutionError> {
    if data.len() <= 1 {
        return Err(ExecutionError::WrongLength)
    } 

    if data[0] != (CellType::Text as u8) {
        return Err(ExecutionError::WrongFormat)
    } 

    let byte_array: [u8; 4] = [data[1], data[2], data[3], data[4]];
    let string_size = u32::from_be_bytes(byte_array);

    if data.len() != ((string_size + 5) as usize) {
        return Err(ExecutionError::WrongLength)
    } 

    let mut bytes: Vec<u8> = Vec::new();
    let mut index: usize = 5;

    while index < data.len() {
        bytes.push(*data.get(index).unwrap());
        index += 1;
    }

    match String::from_utf8(bytes) {
        Ok(new_data) => Ok(new_data),
        Err(_error) => Err(ExecutionError::StringParseFailed)
    }
}

pub fn bin_to_boolean(data: &Vec<u8>) -> Result<bool, ExecutionError> {
    if data.len() != 2 {
        return Err(ExecutionError::WrongLength)
    } 

    if data[0] != (CellType::Boolean as u8) {
        return Err(ExecutionError::WrongFormat)
    } 

    Ok(data[1] == 1u8)
}

pub fn bin_to_unsigned_tinyint(data: &Vec<u8>) -> Result<u8, ExecutionError> {
    if data.len() != 2 {
        return Err(ExecutionError::WrongLength)
    } 

    if data[0] != (CellType::UnsignedTinyint as u8) {
        return Err(ExecutionError::WrongFormat)
    } 
    
    return Ok(data[1]);
}

pub fn bin_to_unsigned_smallint(data: &Vec<u8>) -> Result<u16, ExecutionError> {
    if data.len() != 3 {
        return Err(ExecutionError::WrongLength)
    } 

    if data[0] != (CellType::UnsignedSmallint as u8) {
        return Err(ExecutionError::WrongFormat)
    } 

    let byte_array: [u8; 2] = [data[1], data[2]];
    return Ok(u16::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
}

pub fn bin_to_unsigned_int(data: &Vec<u8>) -> Result<u32, ExecutionError> {
    if data.len() != 5 {
        return Err(ExecutionError::WrongLength)
    } 

    if data[0] != (CellType::UnsignedInt as u8) {
        return Err(ExecutionError::WrongFormat)
    } 

    let byte_array: [u8; 4] = [data[1], data[2], data[3], data[4]];
    return Ok(u32::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
}

pub fn bin_to_unsigned_bigint(data: &Vec<u8>) -> Result<u64, ExecutionError> {
    if data.len() != 9 {
        return Err(ExecutionError::WrongLength)
    } 

    if data[0] != (CellType::UnsignedBigint as u8) {
        return Err(ExecutionError::WrongFormat)
    } 

    let byte_array: [u8; 8] = [
        data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8]
    ];
    return Ok(u64::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
}

pub fn bin_to_signed_tinyint(data: &Vec<u8>) -> Result<i8, ExecutionError> {
    if data.len() != 2 {
        return Err(ExecutionError::WrongLength)
    } 

    if data[0] != (CellType::SignedTinyint as u8) {
        return Err(ExecutionError::WrongFormat)
    } 
    
    return Ok(data[1] as i8);
}

pub fn bin_to_signed_smallint(data: &Vec<u8>) -> Result<i16, ExecutionError> {
    if data.len() != 3 {
        return Err(ExecutionError::WrongLength)
    } 

    if data[0] != (CellType::SignedSmallint as u8) {
        return Err(ExecutionError::WrongFormat)
    } 

    let byte_array: [u8; 2] = [data[1], data[2]];
    return Ok(i16::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
}

pub fn bin_to_signed_int(data: &Vec<u8>) -> Result<i32, ExecutionError> {
    if data.len() != 5 {
        return Err(ExecutionError::WrongLength)
    } 

    if data[0] != (CellType::SignedInt as u8) {
        return Err(ExecutionError::WrongFormat)
    } 

    let byte_array: [u8; 4] = [data[1], data[2], data[3], data[4]];
    return Ok(i32::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
}

pub fn bin_to_signed_bigint(data: &Vec<u8>) -> Result<i64, ExecutionError> {
    if data.len() != 9 {
        return Err(ExecutionError::WrongLength)
    } 

    if data[0] != (CellType::SignedBigint as u8) {
        return Err(ExecutionError::WrongFormat)
    } 

    let byte_array: [u8; 8] = [
        data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8]
    ];
    return Ok(i64::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
}

///////////////////////////////////////////////////////////////////////////////

pub fn get_tuple_database(name: &String) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple_push_varchar(&mut tuple, name);
    return tuple;
}

pub fn get_tuple_table(db_name: &String, name: &String) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple_push_varchar(&mut tuple, db_name);
    tuple_push_varchar(&mut tuple, name);
    tuple_push_varchar(&mut tuple, &String::from("table"));
    tuple_push_varchar(&mut tuple, &String::from(""));
    return tuple;
}

pub fn get_tuple_column(
    id: u64,
    db_name: &String,
    tbl_name: &String,
    name: &String,
    ctype: &String,
    not_null: bool,
    unique: bool,
    primary_key: bool,
    default: &String
) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple_push_unsigned_bigint(&mut tuple, id);
    tuple_push_varchar(&mut tuple, db_name);
    tuple_push_varchar(&mut tuple, tbl_name);
    tuple_push_varchar(&mut tuple, name);
    tuple_push_varchar(&mut tuple, ctype);
    tuple_push_boolean(&mut tuple, not_null);
    tuple_push_boolean(&mut tuple, unique);
    tuple_push_boolean(&mut tuple, primary_key);
    tuple_push_varchar(&mut tuple, default);
    return tuple;
}

pub fn get_tuple_column_without_id(
    db_name: &String,
    tbl_name: &String,
    name: &String,
    ctype: &String,
    not_null: bool,
    unique: bool,
    primary_key: bool,
    default: &String
) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple_push_varchar(&mut tuple, db_name);
    tuple_push_varchar(&mut tuple, tbl_name);
    tuple_push_varchar(&mut tuple, name);
    tuple_push_varchar(&mut tuple, ctype);
    tuple_push_boolean(&mut tuple, not_null);
    tuple_push_boolean(&mut tuple, unique);
    tuple_push_boolean(&mut tuple, primary_key);
    tuple_push_varchar(&mut tuple, default);
    return tuple;
}

pub fn get_tuple_sequence(
    id: u64,
    db_name: &String,
    tbl_name: &String,
    col_name: &String,
    name: &String,
    next_id: u64
) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple_push_unsigned_bigint(&mut tuple, id);
    tuple_push_varchar(&mut tuple, db_name);
    tuple_push_varchar(&mut tuple, tbl_name);
    tuple_push_varchar(&mut tuple, col_name);
    tuple_push_varchar(&mut tuple, name);
    tuple_push_unsigned_bigint(&mut tuple, next_id);
    return tuple;
}

pub fn get_tuple_sequence_without_id(
    db_name: &String,
    tbl_name: &String,
    col_name: &String,
    name: &String,
    next_id: u64
) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple_push_varchar(&mut tuple, db_name);
    tuple_push_varchar(&mut tuple, tbl_name);
    tuple_push_varchar(&mut tuple, col_name);
    tuple_push_varchar(&mut tuple, name);
    tuple_push_unsigned_bigint(&mut tuple, next_id);
    return tuple;
}

pub fn get_tuple_index(
    db_name: &String,
    tbl_name: &String,
    col_name: &String,
    name: &String,
    itype: &String
) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple_push_varchar(&mut tuple, db_name);
    tuple_push_varchar(&mut tuple, tbl_name);
    tuple_push_varchar(&mut tuple, col_name);
    tuple_push_varchar(&mut tuple, name);
    tuple_push_varchar(&mut tuple, itype);
    return tuple;
}


