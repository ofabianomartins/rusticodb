use rusticodb::storage::tuple_new;
use rusticodb::storage::tuple_serialize;
use rusticodb::storage::tuple_deserialize;
use rusticodb::storage::Data;

#[test]
pub fn test_tuple_push_null() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 1u8.to_be_bytes().to_vec());

    let mut tuple = tuple_new();
    tuple.push(Data::Null);

    assert_eq!(tuple_serialize(&tuple), buffer);
}

#[test]
pub fn test_tuple_push_varchar() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 12u8.to_be_bytes().to_vec());
    buffer.append(&mut (bytes_array.len() as u16).to_be_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut tuple = tuple_new();
    tuple.push(Data::Varchar(data));

    assert_eq!(tuple_serialize(&tuple), buffer);
}

#[test]
pub fn test_tuple_push_text() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 13u8.to_be_bytes().to_vec());
    buffer.append(&mut (bytes_array.len() as u32).to_be_bytes().to_vec());
    buffer.append(&mut bytes_array);

    let mut tuple = tuple_new();
    tuple.push(Data::Text(data));

    assert_eq!(tuple_serialize(&tuple), buffer);
}

#[test]
pub fn test_push_boolean_true() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 3u8.to_be_bytes().to_vec());
    buffer.push(1u8);

    let mut tuple = tuple_new();
    tuple.push(Data::Boolean(true));

    assert_eq!(tuple_serialize(&tuple), buffer);
}

#[test]
pub fn test_push_boolean_false() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 3u8.to_be_bytes().to_vec());
    buffer.push(0u8);

    let mut tuple = tuple_new();
    tuple.push(Data::Boolean(false));

    assert_eq!(tuple_serialize(&tuple), buffer);
}

#[test]
pub fn test_push_unsigned_tinyint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 4u8.to_be_bytes().to_vec());
    buffer.append(&mut 50u8.to_be_bytes().to_vec());

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedTinyint(50u8));

    assert_eq!(tuple_serialize(&tuple), buffer);
}

#[test]
pub fn test_push_unsigned_smallint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 5u8.to_be_bytes().to_vec());
    buffer.append(&mut 50u16.to_be_bytes().to_vec());

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedSmallint(50u16));

    assert_eq!(tuple_serialize(&tuple), buffer);
}

#[test]
pub fn test_push_unsigned_int_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 6u8.to_be_bytes().to_vec());
    buffer.append(&mut 50u32.to_be_bytes().to_vec());

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedInt(50u32));

    assert_eq!(tuple_serialize(&tuple), buffer);
}

#[test]
pub fn test_push_unsigned_bigint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 7u8.to_be_bytes().to_vec());
    buffer.append(&mut 50u64.to_be_bytes().to_vec());

    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(50u64));

    assert_eq!(tuple_serialize(&tuple), buffer);
}

#[test]
pub fn test_push_signed_tinyint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 8u8.to_be_bytes().to_vec());
    buffer.append(&mut 50i8.to_be_bytes().to_vec());

    let mut tuple = tuple_new();
    tuple.push(Data::SignedTinyint(50i8));

    assert_eq!(tuple_serialize(&tuple), buffer);
}

#[test]
pub fn test_push_signed_smallint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 9u8.to_be_bytes().to_vec());
    buffer.append(&mut 50i16.to_be_bytes().to_vec());

    let mut tuple = tuple_new();
    tuple.push(Data::SignedSmallint(50i16));

    assert_eq!(tuple_serialize(&tuple), buffer);
}


#[test]
pub fn test_push_signed_int_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 10u8.to_be_bytes().to_vec());
    buffer.append(&mut 50i32.to_be_bytes().to_vec());

    let mut tuple = tuple_new();
    tuple.push(Data::SignedInt(50i32));

    assert_eq!(tuple_serialize(&tuple), buffer);
}

#[test]
pub fn test_push_signed_bigint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 11u8.to_be_bytes().to_vec());
    buffer.append(&mut 50i64.to_be_bytes().to_vec());

    let mut tuple = tuple_new();
    tuple.push(Data::SignedBigint(50i64));

    assert_eq!(tuple_serialize(&tuple), buffer);
}

#[test]
pub fn test_push_two_signed_bigint_to_u8() {
    let mut buffer: Vec<u8> = Vec::new();

    buffer.append(&mut 2u8.to_be_bytes().to_vec());
    buffer.append(&mut 11u8.to_be_bytes().to_vec());
    buffer.append(&mut 11u8.to_be_bytes().to_vec());
    buffer.append(&mut 50i64.to_be_bytes().to_vec());
    buffer.append(&mut 51i64.to_be_bytes().to_vec());

    let mut tuple = tuple_new();
    tuple.push(Data::SignedBigint(50i64));
    tuple.push(Data::SignedBigint(51i64));

    assert_eq!(tuple_serialize(&tuple), buffer);
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


#[test]
pub fn test_tuple_pull_varchar() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 12u8.to_be_bytes().to_vec());
    buffer.append(&mut (bytes_array.len() as u16).to_be_bytes().to_vec());
    buffer.append(&mut bytes_array);

    assert_eq!(tuple_deserialize(&buffer), vec![Data::Varchar(data)]);
}

#[test]
pub fn test_tuple_pull_text() {
    let mut buffer: Vec<u8> = Vec::new();
    let data: String = String::from("simple_string");

    let mut bytes_array = data.clone().into_bytes();

    buffer.append(&mut 1u8.to_be_bytes().to_vec());
    buffer.append(&mut 13u8.to_be_bytes().to_vec());
    buffer.append(&mut (bytes_array.len() as u32).to_be_bytes().to_vec());
    buffer.append(&mut bytes_array);

    assert_eq!(tuple_deserialize(&buffer), vec![Data::Text(data)]);
}
