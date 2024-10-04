use rusticodb::storage::cell::Cell;
use rusticodb::storage::tuple::Tuple;
use rusticodb::storage::os_interface::BLOCK_SIZE;

#[test]
pub fn test_cell_string_to_bin_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.append(&mut 1u64.to_le_bytes().to_vec());
    buffer.append(&mut (bytes_array.len() as u16).to_le_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut cell = Cell::new();
    cell.string_to_bin(&data);

    let mut tuple = Tuple::new();
    tuple.append_cell(cell);

    assert_eq!(tuple.to_raw_data(), raw_buffer);
}

