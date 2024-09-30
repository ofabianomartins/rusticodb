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

    pub fn to_raw_data(&mut self) -> [u8; BLOCK_SIZE] {
        let mut buffer: Vec<u8> = Vec::new();
        let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];

        let size = self.cells.len() as u64;  

        buffer.append(&mut size.to_le_bytes().to_vec());
        for cell in &mut self.cells {
            buffer.append(&mut cell.data);
        }

        for (idx, elem) in &mut buffer.iter().enumerate() {
            raw_buffer[idx] = *elem;
        }
        return raw_buffer;
    }

}
