use std::usize;

use crate::storage::BLOCK_SIZE;
use crate::storage::Tuple;

pub type Page = [u8; BLOCK_SIZE];

pub fn page_new() -> [u8; BLOCK_SIZE] {
    [0u8; BLOCK_SIZE]
}

pub fn page_set_u16_value(page: &mut Page, index: usize, position: u16) {
    if position > 255 {
        page[index] = (position >> 8) as u8;
    }
    page[index+1] = (position % 256) as u8;
}

pub fn page_get_u16_value(page: &Page, index: usize) -> u16 {
    let byte_array: [u8; 2] = [page[index], page[index + 1]];
    return u16::from_be_bytes(byte_array); // or use `from_be_bytes` for big-endian
}

fn page_add_tuple(page: &mut Page, tuple: &Tuple) {
    let tuple_count = page_get_u16_value(page, 0); // getting tuple count
    page_set_u16_value(page, 0, tuple_count + 1); // saving tuple count
    let mut tuple_position: u16 = if tuple_count == 0 {
        BLOCK_SIZE as u16
    } else {
        page_get_u16_value(page, 2*(tuple_count as usize))
    };
    tuple_position = tuple_position - (tuple.len() as u16);

    page_set_u16_value(page, 2*(tuple_count as usize + 1), tuple_position);
    for (idx, elem) in &mut tuple.iter().enumerate() {
        page[(tuple_position as usize) + idx] = *elem;
    }
}

pub fn page_insert_tuples(page: &mut Page, tuples: &mut Vec<Tuple>) {
    for tuple in tuples {
        page_add_tuple(page, tuple);
    }
}

pub fn page_update_tuples(page: &mut Page, tuples: &mut Vec<Tuple>) {
    page_set_u16_value(page, 0, 0);
    for tuple in tuples.iter() {
        page_add_tuple(page, tuple);
    }
}

pub fn page_read_tuples(page: &Page) -> Vec<Tuple> {
    let mut tuples = Vec::new();

    for tuple_index in 0..page_get_u16_value(page, 0) {
        let tuple_position = page_get_u16_value(page, 2*(tuple_index as usize + 1));
        let data_size = page_get_u16_value(page, tuple_position as usize + 2);
        let mut buffer_array: Tuple = Tuple::new();

        for n in tuple_position..(tuple_position + data_size) {
            buffer_array.push(page[n as usize]);
        }
        tuples.push(buffer_array);
    }

    return tuples;
}
