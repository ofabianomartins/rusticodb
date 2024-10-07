use crate::storage::cell::Cell;
use crate::storage::os_interface::BLOCK_SIZE;

#[derive(Debug)]
pub struct Tuple {
    pub cells: Vec<Cell>
}

impl Tuple {

    pub fn new() -> Self {
        Tuple { cells: Vec::new() }
    }

    pub fn append_cell(&mut self, cell: Cell) {
        self.cells.push(cell);
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

    pub fn to_raw_data(&mut self) -> [u8; BLOCK_SIZE] {
        let mut buffer: Vec<u8> = Vec::new();
        let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];

        let size = self.cells.len() as u16;  

        buffer.append(&mut size.to_be_bytes().to_vec());
        for cell in &mut self.cells {
            buffer.append(&mut cell.data);
        }

        for (idx, elem) in &mut buffer.iter().enumerate() {
            raw_buffer[idx] = *elem;
        }
        return raw_buffer;
    }

}
