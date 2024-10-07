use std::collections::HashMap;
use std::usize;

use crate::config::Config;
use crate::storage::tuple::Tuple;
use crate::storage::os_interface::OsInterface;
use crate::storage::os_interface::BLOCK_SIZE;

use super::tuple;

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

    pub fn is_full(&mut self) -> bool {
        return self.size < BLOCK_SIZE;
    }

    pub fn set_tuple_count(&mut self, new_tuple_count: u16) {
        if new_tuple_count > 255 {
            self.data[0] = (new_tuple_count >> 8) as u8;
        }
        self.data[1] = (new_tuple_count % 256) as u8;
    }

    pub fn tuple_count(&mut self) -> u16 {
        let byte_array: [u8; 2] = [self.data[0], self.data[1]];
        return u16::from_be_bytes(byte_array); // or use `from_be_bytes` for big-endian
    }

    pub fn set_next_tuple_position(&mut self, new_nex_tuple_position: u16) {
        if new_nex_tuple_position > 255 {
            self.data[2] = (new_nex_tuple_position >> 8) as u8;
        }
        self.data[3] = (new_nex_tuple_position % 256) as u8;
    }

    pub fn next_tuple_position(&mut self) -> u16 {
        let byte_array: [u8; 2] = [self.data[2], self.data[3]];
        return u16::from_be_bytes(byte_array); // or use `from_be_bytes` for big-endian
    }
}

#[derive(Debug)]
pub struct Pager { 
    pub pages: HashMap<String, Page>
}

impl Pager {
    pub fn new() -> Self {
        Self { pages: HashMap::new() }
    }

    pub fn insert_tuples(&mut self, database_name: &String, table_name: &String, tuples: &mut Vec<Tuple>) {
        let page_key = format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);

        self.pages.entry(page_key.clone()).and_modify(|_| {}).or_insert(Page::new(0));

        self.pages.entry(page_key.clone())
            .and_modify(|page| {
                let tuple_count = page.tuple_count();
                let next_tuple_position = page.next_tuple_position();

                page.set_tuple_count(tuple_count + (tuples.len() as u16));

                let mut buffer: Vec<u8> = Vec::new();
                for tuple in tuples {
                    let size = tuple.cells.len() as u16;  
                    buffer.append(&mut size.to_be_bytes().to_vec());

                    for cell in &mut tuple.cells {
                        buffer.append(&mut cell.data);
                    }
                }

                page.set_next_tuple_position(next_tuple_position + (buffer.len() as u16));

                for (idx, elem) in &mut buffer.iter().enumerate() {
                    (*page).data[(next_tuple_position as usize) + idx] = *elem;
                }
            })
            .or_insert(Page::new(0));
    }


    pub fn write_data(&mut self, database_name: &String, table_name: &String, pos: u64, data: &[u8; BLOCK_SIZE]) {
        let rows_filename = format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);

        OsInterface::write_data(&rows_filename, pos, data);
    }

    pub fn read_data(&mut self, database_name: &String, table_name: &String, pos: u64) -> [u8; BLOCK_SIZE] {
        let rows_filename = format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);

        return OsInterface::read_data(&rows_filename, pos);
    }
}
