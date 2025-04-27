use std::usize;

use crate::storage::BLOCK_SIZE;
use crate::storage::Tuple;
use crate::storage::tuple_display;
use crate::storage::tuple_serialize;
use crate::storage::tuple_deserialize;

use crate::utils::v_u8_to_u16;
use crate::utils::v_u8_to_vec_u8;

#[derive(Debug)]
pub struct Page {
    pub tuples: Vec<Tuple>,
}

pub fn page_new() -> Page {
    Page { tuples: Vec::new() }
}

pub fn page_amount_left(page: &Page) -> u16 {
    let mut block_pointer = BLOCK_SIZE;

    for (_, tuple) in page.tuples.iter().enumerate() {
        let raw_tuple = tuple_serialize(tuple);
        block_pointer -= raw_tuple.len();
    }

    if block_pointer < 2*(page.tuples.len() + 1) {
        return 0;
    };
    return (block_pointer - 2*(page.tuples.len() + 1)) as u16;
}

fn page_add_tuple(page: &mut Page, tuple: &Tuple) {
    page.tuples.push(tuple.clone());
}

pub fn page_insert_tuples(page: &mut Page, tuples: &mut Vec<Tuple>) {
    for tuple in tuples {
        page_add_tuple(page, tuple);
    }
}

pub fn page_update_tuples(page: &mut Page, tuples: &mut Vec<Tuple>) {
    page.tuples = tuples.clone();
}

pub fn page_read_tuple(page: &Page, tuple_index: u16) -> Tuple {
    return page.tuples.get(tuple_index as usize).unwrap().clone();
}

pub fn page_read_tuples(page: &Page) -> Vec<Tuple> {
    let mut tuples = Vec::new();

    for tuple_index in 0..page.tuples.len() {
        tuples.push(page_read_tuple(page, tuple_index as u16));
    }

    return tuples;
}

pub fn page_display(page: &Page) {
    let tuple_count = page.tuples.len();

    print!("Page [{}, (", tuple_count);

    for (idx, tuple) in page_read_tuples(page).iter().enumerate() {
        tuple_display(tuple);

        if idx != tuple_count - 1 {
          print!(",");
        }
    }
    print!(")]")
}

pub fn page_serialize(page: &Page) -> [u8; BLOCK_SIZE] {
    let mut raw_buffer: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];

    let mut block_pointer = BLOCK_SIZE;

    for (tidx, tuple) in page.tuples.iter().enumerate() {
        let raw_tuple = tuple_serialize(tuple);
        block_pointer -= raw_tuple.len();

        raw_buffer[2*(tidx + 1)] = (block_pointer >> 8) as u8;
        raw_buffer[2*(tidx + 1) + 1] = (block_pointer % 256) as u8;

        for (cidx, cell) in raw_tuple.iter().enumerate() {
            raw_buffer[block_pointer + cidx] = *cell;
        }
    }

    raw_buffer[0] = (page.tuples.len() >> 8) as u8;
    raw_buffer[1] = (page.tuples.len() % 255) as u8;
    raw_buffer
}

pub fn page_deserialize(raw_page: [u8; BLOCK_SIZE]) -> Page {
    let tuple_count = v_u8_to_u16(&raw_page, 0) as usize;
    let mut tuples: Vec<Tuple> = Vec::new();

    for tidx in 0..tuple_count {
        let block_pointer = v_u8_to_u16(&raw_page, 2*(tidx + 1)) as usize;
        let next_pointer = if tidx != 0 {
            v_u8_to_u16(&raw_page, 2*(tidx)) as usize
        } else {
            BLOCK_SIZE
        };

        let size = next_pointer - block_pointer;
        let raw_tuple: Vec<u8> = v_u8_to_vec_u8(&raw_page, block_pointer,  size);

        tuples.push(tuple_deserialize(&raw_tuple));
    }

    return Page { tuples };
}
