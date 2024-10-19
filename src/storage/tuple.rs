use crate::storage::cell::Cell;
use crate::storage::os_interface::BLOCK_SIZE;

#[derive(Debug)]
pub struct Tuple {
    pub data: Vec<u8>
}

impl Tuple {

    pub fn new() -> Self {
        let mut data = Vec::new();
        data.push(0);
        data.push(0);
        data.push(0);
        data.push(0);
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
