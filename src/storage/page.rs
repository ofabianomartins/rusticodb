use std::usize;

use crate::storage::BLOCK_SIZE;
use crate::storage::Tuple;
use crate::storage::tuple_display;

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

pub fn page_amount_left(page: &Page) -> u16 {
    let tuple_count = page_get_u16_value(page, 0);
    let last_position = if tuple_count == 0 {
        BLOCK_SIZE as u16
    } else {
        page_get_u16_value(page, 2*(tuple_count as usize))
    };

    return last_position - 2*(tuple_count + 1)
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

pub fn page_read_tuple(page: &Page, tuple_index: u16) -> Tuple {
    let tuple_position = page_get_u16_value(page, 2*(tuple_index as usize + 1));
    let data_size = page_get_u16_value(page, tuple_position as usize + 2);
    let mut new_tuple: Tuple = Tuple::new();

    for n in tuple_position..(tuple_position + data_size) {
        new_tuple.push(page[n as usize]);
    }

    return new_tuple;
}

pub fn page_read_tuples(page: &Page) -> Vec<Tuple> {
    let mut tuples = Vec::new();

    for tuple_index in 0..page_get_u16_value(page, 0) {
        tuples.push(page_read_tuple(page, tuple_index));
    }

    return tuples;
}

pub fn page_display(page: &Page) {
    let tuple_count = page_get_u16_value(page, 0);

    print!("Page [{}, (", tuple_count);

    for (idx, tuple) in page_read_tuples(page).iter().enumerate() {
        tuple_display(tuple);

        if idx as u16 != tuple_count - 1 {
          print!(",");
        }
    }
    print!(")]")
}
