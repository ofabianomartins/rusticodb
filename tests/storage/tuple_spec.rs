use rusticodb::storage::BLOCK_SIZE;
use rusticodb::storage::tuple_new;
use rusticodb::storage::tuple_to_raw_data;
use rusticodb::storage::Data;

#[test]
pub fn test_tuple_push_null() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u16.to_be_bytes().to_vec());
    buffer.append(&mut 5u16.to_be_bytes().to_vec());
    buffer.push(1);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::Null);

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}

#[test]
pub fn test_tuple_push_varchar() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.append(&mut 1u16.to_be_bytes().to_vec());
    buffer.append(&mut 20u16.to_be_bytes().to_vec());
    buffer.push(11);
    buffer.append(&mut (bytes_array.len() as u16).to_be_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::Varchar(data));

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}

#[test]
pub fn test_tuple_push_text() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.append(&mut 1u16.to_be_bytes().to_vec());
    buffer.append(&mut 22u16.to_be_bytes().to_vec());
    buffer.push(12);
    buffer.append(&mut (bytes_array.len() as u32).to_be_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::Varchar(data));

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}

#[test]
pub fn test_push_boolean_true() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u16.to_be_bytes().to_vec());
    buffer.append(&mut 6u16.to_be_bytes().to_vec());
    buffer.push(2);
    buffer.push(1u8);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::Boolean(true));

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}

#[test]
pub fn test_push_boolean_false() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u16.to_be_bytes().to_vec());
    buffer.append(&mut 6u16.to_be_bytes().to_vec());
    buffer.push(2);
    buffer.push(0u8);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::Boolean(false));

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}

#[test]
pub fn test_push_unsigned_tinyint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u16.to_be_bytes().to_vec());
    buffer.append(&mut 6u16.to_be_bytes().to_vec());
    buffer.push(3);
    buffer.push(50u8);

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedTinyint(50u8));

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}

#[test]
pub fn test_push_unsigned_smallint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u16.to_be_bytes().to_vec());
    buffer.append(&mut 7u16.to_be_bytes().to_vec());
    buffer.push(4);
    buffer.append(&mut 50u16.to_be_bytes().to_vec());

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedSmallint(50u16));

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}

#[test]
pub fn test_push_unsigned_int_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u16.to_be_bytes().to_vec());
    buffer.append(&mut 9u16.to_be_bytes().to_vec());
    buffer.push(5);
    buffer.append(&mut 50u32.to_be_bytes().to_vec());

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedInt(50u32));

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}

#[test]
pub fn test_push_unsigned_bigint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u16.to_be_bytes().to_vec());
    buffer.append(&mut 13u16.to_be_bytes().to_vec());
    buffer.push(6);
    buffer.append(&mut 50u64.to_be_bytes().to_vec());

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(50u64));

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}

#[test]
pub fn test_push_signed_tinyint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u16.to_be_bytes().to_vec());
    buffer.append(&mut 6u16.to_be_bytes().to_vec());
    buffer.push(7);
    buffer.append(&mut 50i8.to_be_bytes().to_vec());

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::SignedTinyint(50i8));

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}

#[test]
pub fn test_push_signed_smallint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u16.to_be_bytes().to_vec());
    buffer.append(&mut 7u16.to_be_bytes().to_vec());
    buffer.push(8);
    buffer.append(&mut 50i16.to_be_bytes().to_vec());

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::SignedSmallint(50i16));

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}


#[test]
pub fn test_push_signed_int_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u16.to_be_bytes().to_vec());
    buffer.append(&mut 9u16.to_be_bytes().to_vec());
    buffer.push(9);
    buffer.append(&mut 50i32.to_be_bytes().to_vec());

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::SignedInt(50i32));

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}

#[test]
pub fn test_push_signed_bigint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u16.to_be_bytes().to_vec());
    buffer.append(&mut 13u16.to_be_bytes().to_vec());
    buffer.push(10);
    buffer.append(&mut 50i64.to_be_bytes().to_vec());

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::SignedBigint(50i64));

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}

#[test]
pub fn test_push_two_signed_bigint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 2u16.to_be_bytes().to_vec());
    buffer.append(&mut 22u16.to_be_bytes().to_vec());
    buffer.push(10);
    buffer.append(&mut 50i64.to_be_bytes().to_vec());
    buffer.push(10);
    buffer.append(&mut 51i64.to_be_bytes().to_vec());

    let mut raw_buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    for (idx, elem) in &mut buffer.iter().enumerate() {
        raw_buffer[idx] = *elem;
    }

    let mut tuple = tuple_new();
    tuple.push(Data::SignedBigint(50i64));
    tuple.push(Data::SignedBigint(51i64));

    assert_eq!(tuple_to_raw_data(&tuple), raw_buffer);
}


#[test]
pub fn test_tuple_get_varchar_on_position() {
    let data: String = String::from("simple_string");
    let mut tuple = tuple_new();
    tuple.push(Data::Varchar(data.clone()));

    assert_eq!(tuple.get(0).unwrap().clone(), Data::Varchar(data));
}

#[test]
pub fn test_tuple_get_text_on_position() {
    let data: String = String::from("simple_string");
    let mut tuple = tuple_new();
    tuple.push(Data::Varchar(data.clone()));

    assert_eq!(tuple.get(0).unwrap().clone(), Data::Varchar(data));
}

#[test]
pub fn test_tuple_get_unsigned_tinyint_on_position() {
    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedTinyint(50u8));

    assert_eq!(tuple.get(0).unwrap().clone(), Data::UnsignedTinyint(50u8));
}

#[test]
pub fn test_tuple_get_unsigned_smallint_on_position() {
    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedSmallint(50u16));

    assert_eq!(tuple.get(0).unwrap().clone(), Data::UnsignedSmallint(50u16));
}

#[test]
pub fn test_tuple_get_unsigned_int_on_position() {
    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedInt(50u32));

    assert_eq!(tuple.get(0).unwrap().clone(), Data::UnsignedInt(50u32));
}

#[test]
pub fn test_tuple_get_unsigned_bigint_on_position() {
    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(50u64));

    assert_eq!(tuple.get(0).unwrap().clone(), Data::UnsignedBigint(50u64));
}

#[test]
pub fn test_tuple_get_signed_tinyint_on_position() {
    let mut tuple = tuple_new();
    tuple.push(Data::SignedTinyint(50i8));

    assert_eq!(tuple.get(0).unwrap().clone(), Data::SignedTinyint(50i8));
}

#[test]
pub fn test_tuple_get_signed_smallint_on_position() {
    let mut tuple = tuple_new();
    tuple.push(Data::SignedSmallint(50i16));

    assert_eq!(tuple.get(0).unwrap().clone(), Data::SignedSmallint(50i16));
}

#[test]
pub fn test_tuple_get_signed_int_on_position() {
    let mut tuple = tuple_new();
    tuple.push(Data::SignedInt(50i32));

    assert_eq!(tuple.get(0).unwrap().clone(), Data::SignedInt(50i32));
}

#[test]
pub fn test_tuple_get_signed_bigint_on_position() {
    let mut tuple = tuple_new();
    tuple.push(Data::SignedBigint(50i64));

    assert_eq!(tuple.get(0).unwrap().clone(), Data::SignedBigint(50i64));
}

#[test]
pub fn test_tuple_insert_three_cell_and_get_signed_bigint_on_position() {
    let mut tuple = tuple_new();
    tuple.push(Data::SignedBigint(51i64));
    tuple.push(Data::SignedBigint(52i64));
    tuple.push(Data::SignedBigint(53i64));

    assert_eq!(tuple.get(2).unwrap().clone(), Data::SignedBigint(53i64));
}

#[test]
pub fn test_tuple_insert_two_number_and_one_string_and_get_varchar_on_position() {
    let data: String = String::from("simple_string");
    let mut tuple = tuple_new();
    tuple.push(Data::SignedBigint(51i64));
    tuple.push(Data::Varchar(data.clone()));
    tuple.push(Data::SignedBigint(52i64));

    assert_eq!(tuple.get(1).unwrap().clone(), Data::Varchar(data));
}
