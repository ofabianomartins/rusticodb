use std::usize;
use std::fmt;

use crate::storage::BLOCK_SIZE;
use crate::storage::Tuple;

#[derive(Debug)]
pub struct Page { 
    pub index: u64, 
    pub size: usize, 
    pub data: [u8; BLOCK_SIZE]
}

impl Page {
    pub fn new(index: u64) -> Self {
        let mut empty_data = [0u8; BLOCK_SIZE];
        empty_data[3] = 4;
        Self { index, size: 0, data: empty_data }
    }

    pub fn load(index: u64, data: [u8; BLOCK_SIZE]) -> Self {
        Self { index, size: 0, data }
    }

    pub fn is_full(&mut self) -> bool {
        return self.size < BLOCK_SIZE;
    }

    pub fn set_tuple_count(&mut self, new_tuple_count: u16) {
        if new_tuple_count > 255 {
            self.data[0] = (new_tuple_count >> 8) as u8;
        }
        self.data[1] = (new_tuple_count % 256) as u8;
    }

    pub fn tuple_count(&self) -> u16 {
        let byte_array: [u8; 2] = [self.data[0], self.data[1]];
        return u16::from_be_bytes(byte_array); // or use `from_be_bytes` for big-endian
    }

    pub fn set_next_tuple_position(&mut self, new_nex_tuple_position: u16) {
        if new_nex_tuple_position > 255 {
            self.data[2] = (new_nex_tuple_position >> 8) as u8;
        }
        self.data[3] = (new_nex_tuple_position % 256) as u8;
    }

    pub fn next_tuple_position(&self) -> u16 {
        let byte_array: [u8; 2] = [self.data[2], self.data[3]];
        return u16::from_be_bytes(byte_array); // or use `from_be_bytes` for big-endian
    }

    pub fn insert_tuples(&mut self, tuples: &mut Vec<Tuple>) {
        let tuple_count = self.tuple_count();
        let next_tuple_position = self.next_tuple_position();

        self.set_tuple_count(tuple_count + (tuples.len() as u16));

        let mut buffer: Vec<u8> = Vec::new();
        for tuple in tuples {
            buffer.append(&mut tuple.data);
        }

        self.set_next_tuple_position(next_tuple_position + (buffer.len() as u16));

        for (idx, elem) in &mut buffer.iter().enumerate() {
            (*self).data[(next_tuple_position as usize) + idx] = *elem;
        }
    }

    pub fn update_tuples(&mut self, tuples: &mut Vec<Tuple>) {
        self.set_tuple_count(tuples.len() as u16);

        let mut buffer: Vec<u8> = Vec::new();
        for tuple in tuples {
            buffer.append(&mut tuple.data);
        }

        self.set_next_tuple_position(4u16 + (buffer.len() as u16));

        for (idx, elem) in &mut buffer.iter().enumerate() {
            (*self).data[4usize + idx] = *elem;
        }
    }

    pub fn read_tuples(&self) -> Vec<Tuple> {
        let mut tuples = Vec::new();

        let tuple_count = self.tuple_count();
        let mut tuple_index = 0;
        let mut position_index: u16 = 4;

        while tuple_index < tuple_count {
            let byte_array2: [u8; 2] = [
                self.data[(position_index as usize) + 2],
                self.data[(position_index as usize) + 3]
            ];
            let data_size = u16::from_be_bytes(byte_array2); 

            let mut buffer_array: Vec<u8> = Vec::new();

            for n in position_index..(position_index + data_size) {
                buffer_array.push(self.data[n as usize]);
            }
            tuples.push(Tuple::load(buffer_array));
            tuple_index += 1;
            position_index += data_size;
        }

        return tuples;
    }
}

impl fmt::Display for Page {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let tuple_count: usize = self.tuple_count() as usize;
        let next_position: usize = self.next_tuple_position() as usize;
        let _ = write!(f, "Page [{}, {}, (", tuple_count, next_position);

        for (idx, tuple) in self.read_tuples().iter().enumerate() {
            let _ = write!(f, "{}", tuple);

            if idx != tuple_count - 1 {
              let _ = write!(f, ",");
            }
        }
        write!(f, ")]")
    }
}
