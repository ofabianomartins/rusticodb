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

pub fn tuple_append_cell(tuple: &mut Tuple, mut cell: Cell) {
    tuple_set_cell_count(tuple, tuple_cell_count(tuple) + 1);
    tuple_set_data_size(tuple, tuple_data_size(tuple) + (cell.data_size() as u16));
    tuple.append(&mut cell.data);
}

pub fn tuple_get_cell(tuple: &Tuple, position: u16) -> Cell {
    let cell_count = tuple_cell_count(tuple);

    if position >= cell_count {
        return Cell::new();
    }

    let mut cell_index = 0;
    let mut position_index: usize = 4;
    let mut cell_size: u32;

    loop {
        if position_index >= tuple.len() {
            return Cell::new();
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
    return Cell::load_cell(buffer_array);
}

pub fn tuple_push_null(tuple: &mut Tuple) {
    let mut cell = Cell::new();
    cell.null_to_bin();
    tuple_append_cell(tuple, cell);
}

pub fn tuple_push_varchar(tuple: &mut Tuple, raw_data: &String) {
    let mut cell = Cell::new();
    cell.varchar_to_bin(&raw_data);
    tuple_append_cell(tuple, cell);
}

pub fn tuple_push_text(tuple: &mut Tuple, raw_data: &String) {
    let mut cell = Cell::new();
    cell.text_to_bin(&raw_data);
    tuple_append_cell(tuple, cell);
}

pub fn tuple_push_boolean(tuple: &mut Tuple, value: bool) {
    let mut cell = Cell::new();
    cell.boolean_to_bin(value);
    tuple_append_cell(tuple, cell);
}

pub fn tuple_push_unsigned_tinyint(tuple: &mut Tuple, value: u8) {
    let mut cell = Cell::new();
    cell.unsigned_tinyint_to_bin(value);
    tuple_append_cell(tuple, cell);
}

pub fn tuple_push_unsigned_smallint(tuple: &mut Tuple, value: u16) {
    let mut cell = Cell::new();
    cell.unsigned_smallint_to_bin(value);
    tuple_append_cell(tuple, cell);
}

pub fn tuple_push_unsigned_int(tuple: &mut Tuple, value: u32) {
    let mut cell = Cell::new();
    cell.unsigned_int_to_bin(value);
    tuple_append_cell(tuple, cell);
}

pub fn tuple_push_unsigned_bigint(tuple: &mut Tuple, value: u64) {
    let mut cell = Cell::new();
    cell.unsigned_bigint_to_bin(value);
    tuple_append_cell(tuple, cell);
}

pub fn tuple_push_signed_tinyint(tuple: &mut Tuple, value: i8) {
    let mut cell = Cell::new();
    cell.signed_tinyint_to_bin(value);
    tuple_append_cell(tuple, cell);
}

pub fn tuple_push_signed_smallint(tuple: &mut Tuple, value: i16) {
    let mut cell = Cell::new();
    cell.signed_smallint_to_bin(value);
    tuple_append_cell(tuple, cell);
}

pub fn tuple_push_signed_int(tuple: &mut Tuple, value: i32) {
    let mut cell = Cell::new();
    cell.signed_int_to_bin(value);
    tuple_append_cell(tuple, cell);
}

pub fn tuple_push_signed_bigint(tuple: &mut Tuple, value: i64) {
    let mut cell = Cell::new();
    cell.signed_bigint_to_bin(value);
    tuple_append_cell(tuple, cell);
}

pub fn tuple_get_vec_u8(tuple: &Tuple, position: u16) -> Result<Vec<u8>, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(Vec::new());
    }

    return tuple_get_cell(tuple, position).get_bin();
}

pub fn tuple_get_varchar(tuple: &Tuple, position: u16) -> Result<String, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(String::from(""));
    }

    return tuple_get_cell(tuple, position).bin_to_varchar();
}

pub fn tuple_get_text(tuple: &Tuple, position: u16) -> Result<String, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(String::from(""));
    }

    return tuple_get_cell(tuple, position).bin_to_text();
}

pub fn tuple_get_boolean(tuple: &Tuple, position: u16) -> Result<bool, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(false);
    }

    return tuple_get_cell(tuple, position).bin_to_boolean();
}

pub fn tuple_get_unsigned_tinyint(tuple: &Tuple, position: u16) -> Result<u8, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return tuple_get_cell(tuple, position).bin_to_unsigned_tinyint();
}

pub fn tuple_get_unsigned_smallint(tuple: &Tuple, position: u16) -> Result<u16, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return tuple_get_cell(tuple, position).bin_to_unsigned_smallint();
}

pub fn tuple_get_unsigned_int(tuple: &Tuple, position: u16) -> Result<u32, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return tuple_get_cell(tuple, position).bin_to_unsigned_int();
}

pub fn tuple_get_unsigned_bigint(tuple: &Tuple, position: u16) -> Result<u64, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return tuple_get_cell(tuple, position).bin_to_unsigned_bigint();
}

pub fn tuple_get_signed_tinyint(tuple: &Tuple, position: u16) -> Result<i8, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return tuple_get_cell(tuple, position).bin_to_signed_tinyint();
}

pub fn tuple_get_signed_smallint(tuple: &Tuple, position: u16) -> Result<i16, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return tuple_get_cell(tuple, position).bin_to_signed_smallint();
}

pub fn tuple_get_signed_int(tuple: &Tuple, position: u16) -> Result<i32, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return tuple_get_cell(tuple, position).bin_to_signed_int();
}

pub fn tuple_get_signed_bigint(tuple: &Tuple, position: u16) -> Result<i64, ExecutionError> {
    if position >= tuple_cell_count(tuple) {
        return Ok(0);
    }

    return tuple_get_cell(tuple, position).bin_to_signed_bigint();
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
        print!("{}", tuple_get_cell(tuple, cell_index));

        if cell_index != cell_count - 1 {
          print!(",");
        }

        cell_index += 1;
    }
    print!(")]")
}

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


