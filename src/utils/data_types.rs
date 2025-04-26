
pub fn vec_u8_to_u16(bytes: &Vec<u8>, pos: usize) -> u16 {
    assert!(bytes.len() < 2, "Input Vec must have exactly 2 elements");

    u16::from_be_bytes([bytes[pos], bytes[pos + 1]])
}

pub fn vec_u8_to_u32(bytes: &Vec<u8>, pos: usize) -> u32 {
    assert!(bytes.len() < 4, "Input Vec must have exactly 4 elements");

    u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
}

pub fn vec_u8_to_u64(bytes: &Vec<u8>, pos: usize) -> u64 {
    u64::from_be_bytes([
        bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3], 
        bytes[pos+4], bytes[pos + 5], bytes[pos + 6], bytes[pos + 7]
    ])
}

pub fn vec_u8_to_i16(bytes: &Vec<u8>, pos: usize) -> i16 {
    i16::from_be_bytes([bytes[pos], bytes[pos + 1]])
}

pub fn vec_u8_to_i32(bytes: &Vec<u8>, pos: usize) -> i32 {
    i32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
}

pub fn vec_u8_to_i64(bytes: &Vec<u8>, pos: usize) -> i64 {
    i64::from_be_bytes([
        bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3], 
        bytes[pos+4], bytes[pos + 5], bytes[pos + 6], bytes[pos + 7]
    ])
}

pub fn vec_u8_to_string(bytes: &Vec<u8>, pos: usize) -> String {
    // Read first 2 bytes as u16 length (little-endian)
    let size = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]) as usize;
    // Extract the string part
    let string_bytes = &bytes[(pos + 2)..(pos + 2 + size)];
    let string = String::from_utf8(string_bytes.to_vec()).map_err(|_| "Invalid UTF-8").unwrap();

    string
}

pub fn vec_u8_to_text(bytes: &Vec<u8>, pos: usize) -> String {
    // Read first 2 bytes as u16 length (little-endian)
    let size = u32::from_be_bytes([
        bytes[pos], bytes[pos + 1],
        bytes[pos + 2], bytes[pos + 3]
    ]) as usize;
    // Extract the string part
    let string_bytes = &bytes[(pos + 4)..(pos + 4 + size)];
    let string = String::from_utf8(string_bytes.to_vec()).map_err(|_| "Invalid UTF-8").unwrap();

    string
}

pub fn v_u8_to_u16(bytes: &[u8], pos: usize) -> u16 {
    u16::from_be_bytes([bytes[pos], bytes[pos + 1]])
}

pub fn v_u8_to_u32(bytes: &[u8], pos: usize) -> u32 {
    u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
}

pub fn v_u8_to_u64(bytes: &[u8], pos: usize) -> u64 {
    u64::from_be_bytes([
        bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3], 
        bytes[pos+4], bytes[pos + 5], bytes[pos + 6], bytes[pos + 7]
    ])
}

pub fn v_u8_to_i16(bytes: &[u8], pos: usize) -> i16 {
    i16::from_be_bytes([bytes[pos], bytes[pos + 1]])
}

pub fn v_u8_to_i32(bytes: &[u8], pos: usize) -> i32 {
    i32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
}

pub fn v_u8_to_i64(bytes: &[u8], pos: usize) -> i64 {
    i64::from_be_bytes([
        bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3], 
        bytes[pos+4], bytes[pos + 5], bytes[pos + 6], bytes[pos + 7]
    ])
}

pub fn v_u8_to_string(bytes: &[u8], pos: usize) -> String {
    // Read first 2 bytes as u16 length (little-endian)
    let size = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]) as usize;
    // Extract the string part
    let string_bytes = &bytes[(pos + 2)..(pos + 2 + size)];
    let string = String::from_utf8(string_bytes.to_vec()).map_err(|_| "Invalid UTF-8").unwrap();

    string
}

pub fn v_u8_to_text(bytes: &[u8], pos: usize) -> String {
    // Read first 2 bytes as u16 length (little-endian)
    let size = u32::from_be_bytes([
        bytes[pos], bytes[pos + 1],
        bytes[pos + 2], bytes[pos + 3]
    ]) as usize;
    // Extract the string part
    let string_bytes = &bytes[(pos + 4)..(pos + 4 + size)];
    let string = String::from_utf8(string_bytes.to_vec()).map_err(|_| "Invalid UTF-8").unwrap();

    string
}

pub fn v_u8_to_vec_u8(bytes: &[u8], pos: usize, size: usize) -> Vec<u8> {
    let mut buffer = Vec::new();

    for idx in 0..size {
        buffer.push(bytes[pos + idx])
    }

    buffer
}
