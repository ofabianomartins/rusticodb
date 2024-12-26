use std::fmt;

use crate::storage::cell::Cell;
use crate::utils::execution_error::ExecutionError;
use crate::storage::os_interface::BLOCK_SIZE;

use super::cell::CellType;

#[derive(Debug, Clone)]
pub struct Tuple {
    pub data: Vec<u8>
}

impl Tuple {

    pub fn new() -> Self {
        let mut data = Vec::new();
        data.push(0);
        data.push(0);
        data.push(0);
        data.push(4);
        Tuple { data }
    }

    pub fn load(data: Vec<u8>) -> Self {
        Tuple { data }
    }

    pub fn append_cell(&mut self, mut cell: Cell) {
        self.set_cell_count(self.cell_count() + 1);
        self.set_data_size(self.data_size() + (cell.data_size() as u16));
        self.data.append(&mut cell.data);
    }

    pub fn get_cell(&self, position: u16) -> Cell {
        let cell_count = self.cell_count();

        if position >= cell_count {
            return Cell::new();
        }

        let mut cell_index = 0;
        let mut position_index: usize = 4;
        let mut cell_size: u32;

        loop {
            if self.data[position_index as usize] == (CellType::String as u8) {
                let byte_array: [u8; 2] = [self.data[position_index + 1], self.data[position_index + 2]];
                cell_size = (u16::from_be_bytes(byte_array) as u32) + 3u32; // or use `from_be_bytes` for big-endian
            } else if self.data[position_index as usize] == (CellType::Text as u8) {
                let byte_array: [u8; 4] = [
                    self.data[position_index + 1], self.data[position_index + 2],
                    self.data[position_index + 3], self.data[position_index + 4]
                ];
                cell_size = u32::from_be_bytes(byte_array) + 5u32; // or use `from_be_bytes` for big-endian
            } else {
                cell_size = Cell::count_data_size(self.data[position_index as usize]);
            }

            if cell_index == position {
                break;
            }

            cell_index += 1;
            position_index += cell_size as usize;
        }

        let mut buffer_array: Vec<u8> = Vec::new();
        for n in position_index..(position_index + (cell_size as usize)) {
            buffer_array.push(self.data[n as usize]);
        }
        return Cell::load_cell(buffer_array);
    }

    pub fn push_null(&mut self) {
        let mut cell = Cell::new();
        cell.null_to_bin();
        self.append_cell(cell);
    }

    pub fn push_string(&mut self, raw_data: &String) {
        let mut cell = Cell::new();
        cell.string_to_bin(&raw_data);
        self.append_cell(cell);
    }

    pub fn push_text(&mut self, raw_data: &String) {
        let mut cell = Cell::new();
        cell.text_to_bin(&raw_data);
        self.append_cell(cell);
    }

    pub fn push_boolean(&mut self, value: bool) {
        let mut cell = Cell::new();
        cell.boolean_to_bin(value);
        self.append_cell(cell);
    }

    pub fn push_unsigned_tinyint(&mut self, value: u8) {
        let mut cell = Cell::new();
        cell.unsigned_tinyint_to_bin(value);
        self.append_cell(cell);
    }

    pub fn push_unsigned_smallint(&mut self, value: u16) {
        let mut cell = Cell::new();
        cell.unsigned_smallint_to_bin(value);
        self.append_cell(cell);
    }

    pub fn push_unsigned_int(&mut self, value: u32) {
        let mut cell = Cell::new();
        cell.unsigned_int_to_bin(value);
        self.append_cell(cell);
    }

    pub fn push_unsigned_bigint(&mut self, value: u64) {
        let mut cell = Cell::new();
        cell.unsigned_bigint_to_bin(value);
        self.append_cell(cell);
    }

    pub fn push_signed_tinyint(&mut self, value: i8) {
        let mut cell = Cell::new();
        cell.signed_tinyint_to_bin(value);
        self.append_cell(cell);
    }

    pub fn push_signed_smallint(&mut self, value: i16) {
        let mut cell = Cell::new();
        cell.signed_smallint_to_bin(value);
        self.append_cell(cell);
    }

    pub fn push_signed_int(&mut self, value: i32) {
        let mut cell = Cell::new();
        cell.signed_int_to_bin(value);
        self.append_cell(cell);
    }

    pub fn push_signed_bigint(&mut self, value: i64) {
        let mut cell = Cell::new();
        cell.signed_bigint_to_bin(value);
        self.append_cell(cell);
    }

    pub fn get_vec_u8(&self, position: u16) -> Result<Vec<u8>, ExecutionError> {
        if position >= self.cell_count() {
            return Ok(Vec::new());
        }

        return self.get_cell(position).get_bin();
    }

    pub fn get_string(&self, position: u16) -> Result<String, ExecutionError> {
        if position >= self.cell_count() {
            return Ok(String::from(""));
        }

        return self.get_cell(position).bin_to_string();
    }

    pub fn get_text(&self, position: u16) -> Result<String, ExecutionError> {
        if position >= self.cell_count() {
            return Ok(String::from(""));
        }

        return self.get_cell(position).bin_to_text();
    }

    pub fn get_boolean(&self, position: u16) -> Result<bool, ExecutionError> {
        if position >= self.cell_count() {
            return Ok(false);
        }

        return self.get_cell(position).bin_to_boolean();
    }

    pub fn get_unsigned_tinyint(&self, position: u16) -> Result<u8, ExecutionError> {
        if position >= self.cell_count() {
            return Ok(0);
        }

        return self.get_cell(position).bin_to_unsigned_tinyint();
    }

    pub fn get_unsigned_smallint(&self, position: u16) -> Result<u16, ExecutionError> {
        if position >= self.cell_count() {
            return Ok(0);
        }

        return self.get_cell(position).bin_to_unsigned_smallint();
    }

    pub fn get_unsigned_int(&self, position: u16) -> Result<u32, ExecutionError> {
        if position >= self.cell_count() {
            return Ok(0);
        }

        return self.get_cell(position).bin_to_unsigned_int();
    }

    pub fn get_unsigned_bigint(&self, position: u16) -> Result<u64, ExecutionError> {
        if position >= self.cell_count() {
            return Ok(0);
        }

        return self.get_cell(position).bin_to_unsigned_bigint();
    }

    pub fn get_signed_tinyint(&self, position: u16) -> Result<i8, ExecutionError> {
        if position >= self.cell_count() {
            return Ok(0);
        }

        return self.get_cell(position).bin_to_signed_tinyint();
    }

    pub fn get_signed_smallint(&self, position: u16) -> Result<i16, ExecutionError> {
        if position >= self.cell_count() {
            return Ok(0);
        }

        return self.get_cell(position).bin_to_signed_smallint();
    }

    pub fn get_signed_int(&self, position: u16) -> Result<i32, ExecutionError> {
        if position >= self.cell_count() {
            return Ok(0);
        }

        return self.get_cell(position).bin_to_signed_int();
    }

    pub fn get_signed_bigint(&self, position: u16) -> Result<i64, ExecutionError> {
        if position >= self.cell_count() {
            return Ok(0);
        }

        return self.get_cell(position).bin_to_signed_bigint();
    }

    pub fn set_cell_count(&mut self, new_cell_count: u16) {
        if new_cell_count > 255 {
            self.data[0] = (new_cell_count >> 8) as u8;
        }
        self.data[1] = (new_cell_count % 256) as u8;
    }

    pub fn cell_count(&self) -> u16 {
        let byte_array: [u8; 2] = [self.data[0], self.data[1]];
        return u16::from_be_bytes(byte_array); // or use `from_be_bytes` for big-endian
    }

    pub fn set_data_size(&mut self, new_data_size: u16) {
        if new_data_size > 255 {
            self.data[2] = (new_data_size >> 8) as u8;
        }
        self.data[3] = (new_data_size % 256) as u8;
    }

    pub fn data_size(&self) -> u16 {
        let byte_array: [u8; 2] = [self.data[2], self.data[3]];
        return u16::from_be_bytes(byte_array); // or use `from_be_bytes` for big-endian
    }

    pub fn to_raw_data(&mut self) -> [u8; BLOCK_SIZE] {
        let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];

        for (idx, elem) in &mut self.data.iter().enumerate() {
            raw_buffer[idx] = *elem;
        }
        return raw_buffer;
    }
}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let cell_count = self.cell_count();
        let mut cell_index = 0;

        while cell_index < cell_count {
            let _ = write!(f, "{}", self.get_cell(cell_index));

            cell_index += 1;
        }
        write!(f, "")
    }
}
