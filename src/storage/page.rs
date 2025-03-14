use std::usize;

use crate::storage::BLOCK_SIZE;
use crate::storage::Tuple;

pub type Page = [u8; BLOCK_SIZE];

pub fn page_new(_index: u64) -> [u8; BLOCK_SIZE] {
    let mut empty_data = [0u8; BLOCK_SIZE];
    empty_data[3] = 4;
    return empty_data;
}

pub fn page_set_tuple_count(page: &mut Page, new_tuple_count: u16) {
    if new_tuple_count > 255 {
       page[0] = (new_tuple_count >> 8) as u8;
    }
    page[1] = (new_tuple_count % 256) as u8;
}

pub fn page_tuple_count(page: &Page) -> u16 {
    let byte_array: [u8; 2] = [page[0], page[1]];
    return u16::from_be_bytes(byte_array); // or use `from_be_bytes` for big-endian
}

pub fn page_set_next_tuple_position(page: &mut Page, new_nex_tuple_position: u16) {
    if new_nex_tuple_position > 255 {
        page[2] = (new_nex_tuple_position >> 8) as u8;
    }
    page[3] = (new_nex_tuple_position % 256) as u8;
}

pub fn page_next_tuple_position(page: &Page) -> u16 {
    let byte_array: [u8; 2] = [page[2], page[3]];
    return u16::from_be_bytes(byte_array); // or use `from_be_bytes` for big-endian
}

pub fn page_insert_tuples(page: &mut Page, tuples: &mut Vec<Tuple>) {
    let tuple_count = page_tuple_count(page);
    let next_tuple_position = page_next_tuple_position(page);

    page_set_tuple_count(page, tuple_count + (tuples.len() as u16));

    let mut buffer: Vec<u8> = Vec::new();
    for tuple in tuples {
        buffer.append(tuple);
    }

    page_set_next_tuple_position(page, next_tuple_position + (buffer.len() as u16));

    for (idx, elem) in &mut buffer.iter().enumerate() {
        page[(next_tuple_position as usize) + idx] = *elem;
    }
}

pub fn page_update_tuples(page: &mut Page, tuples: &mut Vec<Tuple>) {
    page_set_tuple_count(page, tuples.len() as u16);

    let mut buffer: Vec<u8> = Vec::new();
    for tuple in tuples {
        buffer.append(tuple);
    }

    page_set_next_tuple_position(page, 4u16 + (buffer.len() as u16));

    for (idx, elem) in &mut buffer.iter().enumerate() {
        page[4usize + idx] = *elem;
    }
}

pub fn page_read_tuples(page: &Page) -> Vec<Tuple> {
    let mut tuples = Vec::new();

    let tuple_count = page_tuple_count(page);
    let mut tuple_index = 0;
    let mut position_index: u16 = 4;

    while tuple_index < tuple_count {
        let byte_array2: [u8; 2] = [
            page[(position_index as usize) + 2],
            page[(position_index as usize) + 3]
        ];
        let data_size = u16::from_be_bytes(byte_array2); 

        let mut buffer_array: Vec<u8> = Vec::new();

        for n in position_index..(position_index + data_size) {
            buffer_array.push(page[n as usize]);
        }
        tuples.push(buffer_array);
        tuple_index += 1;
        position_index += data_size;
    }

    return tuples;
}
