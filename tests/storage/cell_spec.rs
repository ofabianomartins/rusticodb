use rusticodb::storage::cell::Cell;

#[test]
pub fn test_cell_insert_string_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.append(&mut (bytes_array.len() as u16).to_le_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut cell = Cell::new();

    cell.string_to_bin(&data);

    assert_eq!(cell.data, buffer);

}

