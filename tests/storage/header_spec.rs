use rusticodb::storage::header_new;
use rusticodb::storage::header_serialize;
use rusticodb::storage::header_deserialize;
use rusticodb::storage::BLOCK_SIZE;

#[test]
pub fn test_empty_header() {
    let header = header_new();

    let mut buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    buffer[2] = 1u8;

    assert_eq!(header_serialize(&header), buffer);
}

#[test]
pub fn test_header_serialize_with_diferent_page_count() {
    let mut header = header_new();
    header.page_count = 10;

    let mut buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    buffer[0] = 10u8;
    buffer[2] = 1u8;

    assert_eq!(header_serialize(&header), buffer);
}

#[test]
pub fn test_header_deserialize_weith_diferent_page_count() {
    let mut header = header_new();
    header.page_count = 10;

    let mut buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    buffer[0] = 10u8;
    buffer[2] = 1u8;

    assert_eq!(header_deserialize(&buffer), header);
}
