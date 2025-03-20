use std::usize;

use crate::storage::BLOCK_SIZE;

pub type Header = [u8; BLOCK_SIZE];

pub fn header_new() -> [u8; BLOCK_SIZE] {
    [0u8; BLOCK_SIZE]
}

pub fn header_set_u16_value(page: &mut Header, index: usize, position: u16) {
    if position > 255 {
        page[index] = (position >> 8) as u8;
    }
    page[index+1] = (position % 256) as u8;
}

pub fn header_get_u16_value(page: &Header, index: usize) -> u16 {
    let byte_array: [u8; 2] = [page[index], page[index + 1]];
    return u16::from_be_bytes(byte_array); // or use `from_be_bytes` for big-endian
}

pub fn header_set_page_count(header: &mut Header, value: u16) {
    header_set_u16_value(header, 0, value);
}

pub fn header_page_count(header: &Header) -> u16 {
    return header_get_u16_value(header, 0);
}
