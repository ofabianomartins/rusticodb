use rstest::rstest;

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

#[rstest]
#[case(Data::Null, Data::Null, Data::Boolean(true))]
#[case(Data::Null, Data::UnsignedTinyint(2), Data::Boolean(false))]
#[case(Data::Null, Data::UnsignedSmallint(2), Data::Boolean(false))]
#[case(Data::Null, Data::UnsignedInt(2), Data::Boolean(false))]
#[case(Data::Null, Data::UnsignedBigint(2), Data::Boolean(false))]
#[case(Data::Null, Data::SignedTinyint(2), Data::Boolean(false))]
#[case(Data::Null, Data::SignedSmallint(2), Data::Boolean(false))]
#[case(Data::Null, Data::SignedInt(2), Data::Boolean(false))]
#[case(Data::Null, Data::SignedBigint(2), Data::Boolean(false))]
#[case(Data::Null, Data::Boolean(true), Data::Boolean(false))]
#[case(Data::Null, Data::Boolean(false), Data::Boolean(false))]
#[case(Data::Null, Data::Varchar("".to_string()), Data::Boolean(false))]
#[case(Data::UnsignedTinyint(2), Data::Null, Data::Boolean(false))]
#[case(Data::UnsignedSmallint(2), Data::Null, Data::Boolean(false))]
#[case(Data::UnsignedInt(2), Data::Null, Data::Boolean(false))]
#[case(Data::UnsignedBigint(2), Data::Null, Data::Boolean(false))]
#[case(Data::SignedTinyint(2), Data::Null, Data::Boolean(false))]
#[case(Data::SignedSmallint(2), Data::Null, Data::Boolean(false))]
#[case(Data::SignedInt(2), Data::Null, Data::Boolean(false))]
#[case(Data::SignedBigint(2), Data::Null, Data::Boolean(false))]
#[case(Data::Undefined, Data::Undefined, Data::Boolean(true))]
#[case(Data::Undefined, Data::UnsignedTinyint(2), Data::Boolean(false))]
#[case(Data::Undefined, Data::UnsignedSmallint(2), Data::Boolean(false))]
#[case(Data::Undefined, Data::UnsignedInt(2), Data::Boolean(false))]
#[case(Data::Undefined, Data::UnsignedBigint(2), Data::Boolean(false))]
#[case(Data::Undefined, Data::SignedTinyint(2), Data::Boolean(false))]
#[case(Data::Undefined, Data::SignedSmallint(2), Data::Boolean(false))]
#[case(Data::Undefined, Data::SignedInt(2), Data::Boolean(false))]
#[case(Data::Undefined, Data::SignedBigint(2), Data::Boolean(false))]
#[case(Data::Undefined, Data::Boolean(true), Data::Boolean(false))]
#[case(Data::Undefined, Data::Boolean(false), Data::Boolean(false))]
#[case(Data::Undefined, Data::Varchar("".to_string()), Data::Boolean(false))]
#[case(Data::UnsignedTinyint(2), Data::Undefined, Data::Boolean(false))]
#[case(Data::UnsignedSmallint(2), Data::Undefined, Data::Boolean(false))]
#[case(Data::UnsignedInt(2), Data::Undefined, Data::Boolean(false))]
#[case(Data::UnsignedBigint(2), Data::Undefined, Data::Boolean(false))]
#[case(Data::SignedTinyint(2), Data::Undefined, Data::Boolean(false))]
#[case(Data::SignedSmallint(2), Data::Undefined, Data::Boolean(false))]
#[case(Data::SignedInt(2), Data::Undefined, Data::Boolean(false))]
#[case(Data::SignedBigint(2), Data::Undefined, Data::Boolean(false))]
#[case(Data::UnsignedTinyint(2), Data::UnsignedTinyint(2), Data::Boolean(true))]
#[case(Data::UnsignedSmallint(2), Data::UnsignedSmallint(2), Data::Boolean(true))]
#[case(Data::UnsignedInt(2), Data::UnsignedInt(2), Data::Boolean(true))]
#[case(Data::UnsignedBigint(2), Data::UnsignedBigint(2), Data::Boolean(true))]
#[case(Data::SignedTinyint(2), Data::SignedTinyint(2), Data::Boolean(true))]
#[case(Data::SignedSmallint(2), Data::SignedSmallint(2), Data::Boolean(true))]
#[case(Data::SignedInt(2), Data::SignedInt(2), Data::Boolean(true))]
#[case(Data::SignedBigint(2), Data::SignedBigint(2), Data::Boolean(true))]
#[case(Data::UnsignedTinyint(0), Data::UnsignedTinyint(2), Data::Boolean(false))]
#[case(Data::UnsignedSmallint(0), Data::UnsignedSmallint(2), Data::Boolean(false))]
#[case(Data::UnsignedInt(0), Data::UnsignedInt(2), Data::Boolean(false))]
#[case(Data::UnsignedBigint(0), Data::UnsignedBigint(2), Data::Boolean(false))]
#[case(Data::SignedTinyint(0), Data::SignedTinyint(2), Data::Boolean(false))]
#[case(Data::SignedSmallint(0), Data::SignedSmallint(2), Data::Boolean(false))]
#[case(Data::SignedInt(0), Data::SignedInt(2), Data::Boolean(false))]
#[case(Data::SignedBigint(0), Data::SignedBigint(2), Data::Boolean(false))]
#[case(Data::UnsignedTinyint(2), Data::UnsignedTinyint(0), Data::Boolean(false))]
#[case(Data::UnsignedSmallint(2), Data::UnsignedSmallint(0), Data::Boolean(false))]
#[case(Data::UnsignedInt(2), Data::UnsignedInt(0), Data::Boolean(false))]
#[case(Data::UnsignedBigint(2), Data::UnsignedBigint(0), Data::Boolean(false))]
#[case(Data::SignedTinyint(2), Data::SignedTinyint(0), Data::Boolean(false))]
#[case(Data::SignedSmallint(2), Data::SignedSmallint(0), Data::Boolean(false))]
#[case(Data::SignedInt(2), Data::SignedInt(0), Data::Boolean(false))]
#[case(Data::SignedBigint(2), Data::SignedBigint(0), Data::Boolean(false))]
#[case(Data::UnsignedTinyint(0), Data::UnsignedTinyint(0), Data::Boolean(false))]
#[case(Data::UnsignedSmallint(0), Data::UnsignedSmallint(0), Data::Boolean(false))]
#[case(Data::UnsignedInt(0), Data::UnsignedInt(0), Data::Boolean(false))]
#[case(Data::UnsignedBigint(0), Data::UnsignedBigint(0), Data::Boolean(false))]
#[case(Data::SignedTinyint(0), Data::SignedTinyint(0), Data::Boolean(false))]
#[case(Data::SignedSmallint(0), Data::SignedSmallint(0), Data::Boolean(false))]
#[case(Data::SignedInt(0), Data::SignedInt(0), Data::Boolean(false))]
#[case(Data::SignedBigint(0), Data::SignedBigint(0), Data::Boolean(false))]
#[case(Data::Boolean(true), Data::Boolean(true), Data::Boolean(true))]
#[case(Data::Boolean(true), Data::Boolean(false), Data::Boolean(false))]
#[case(Data::Boolean(false), Data::Boolean(true), Data::Boolean(false))]
#[case(Data::Boolean(false), Data::Boolean(false), Data::Boolean(false))]
#[case(Data::Varchar("".to_string()), Data::Varchar("".to_string()), Data::Boolean(false))]
#[case(Data::Varchar("a".to_string()), Data::Varchar("a".to_string()), Data::Boolean(true))]
#[case(Data::Text("".to_string()), Data::Text("".to_string()), Data::Boolean(false))]
#[case(Data::Text("a".to_string()), Data::Text("a".to_string()), Data::Boolean(true))]
pub fn test_data_combined_by_and_function(#[case] first: Data, #[case] second: Data, #[case] expected: Data) {
    assert!(first.and(&second) == expected, "{}", format!("{:?} with {:?} should be {:?}", first, second, expected));
}

#[rstest]
#[case(Data::Null, Data::Null, Data::Boolean(true))]
#[case(Data::Null, Data::UnsignedTinyint(2), Data::Boolean(true))]
#[case(Data::Null, Data::UnsignedSmallint(2), Data::Boolean(true))]
#[case(Data::Null, Data::UnsignedInt(2), Data::Boolean(true))]
#[case(Data::Null, Data::UnsignedBigint(2), Data::Boolean(true))]
#[case(Data::Null, Data::SignedTinyint(2), Data::Boolean(true))]
#[case(Data::Null, Data::SignedSmallint(2), Data::Boolean(true))]
#[case(Data::Null, Data::SignedInt(2), Data::Boolean(true))]
#[case(Data::Null, Data::SignedBigint(2), Data::Boolean(true))]
#[case(Data::Null, Data::Boolean(true), Data::Boolean(true))]
#[case(Data::Null, Data::Boolean(false), Data::Boolean(true))]
#[case(Data::Null, Data::Varchar("".to_string()), Data::Boolean(true))]
#[case(Data::Null, Data::Text("".to_string()), Data::Boolean(true))]
#[case(Data::UnsignedTinyint(2), Data::Null, Data::Boolean(true))]
#[case(Data::UnsignedSmallint(2), Data::Null, Data::Boolean(true))]
#[case(Data::UnsignedInt(2), Data::Null, Data::Boolean(true))]
#[case(Data::UnsignedBigint(2), Data::Null, Data::Boolean(true))]
#[case(Data::SignedTinyint(2), Data::Null, Data::Boolean(true))]
#[case(Data::SignedSmallint(2), Data::Null, Data::Boolean(true))]
#[case(Data::SignedInt(2), Data::Null, Data::Boolean(true))]
#[case(Data::SignedBigint(2), Data::Null, Data::Boolean(true))]
#[case(Data::Undefined, Data::Undefined, Data::Boolean(true))]
#[case(Data::Undefined, Data::UnsignedTinyint(2), Data::Boolean(true))]
#[case(Data::Undefined, Data::UnsignedSmallint(2), Data::Boolean(true))]
#[case(Data::Undefined, Data::UnsignedInt(2), Data::Boolean(true))]
#[case(Data::Undefined, Data::UnsignedBigint(2), Data::Boolean(true))]
#[case(Data::Undefined, Data::SignedTinyint(2), Data::Boolean(true))]
#[case(Data::Undefined, Data::SignedSmallint(2), Data::Boolean(true))]
#[case(Data::Undefined, Data::SignedInt(2), Data::Boolean(true))]
#[case(Data::Undefined, Data::SignedBigint(2), Data::Boolean(true))]
#[case(Data::Undefined, Data::Boolean(true), Data::Boolean(true))]
#[case(Data::Undefined, Data::Boolean(false), Data::Boolean(true))]
#[case(Data::Undefined, Data::Varchar("".to_string()), Data::Boolean(true))]
#[case(Data::UnsignedTinyint(2), Data::Undefined, Data::Boolean(true))]
#[case(Data::UnsignedSmallint(2), Data::Undefined, Data::Boolean(true))]
#[case(Data::UnsignedInt(2), Data::Undefined, Data::Boolean(true))]
#[case(Data::UnsignedBigint(2), Data::Undefined, Data::Boolean(true))]
#[case(Data::SignedTinyint(2), Data::Undefined, Data::Boolean(true))]
#[case(Data::SignedSmallint(2), Data::Undefined, Data::Boolean(true))]
#[case(Data::SignedInt(2), Data::Undefined, Data::Boolean(true))]
#[case(Data::SignedBigint(2), Data::Undefined, Data::Boolean(true))]
#[case(Data::UnsignedTinyint(2), Data::UnsignedTinyint(2), Data::Boolean(true))]
#[case(Data::UnsignedSmallint(2), Data::UnsignedSmallint(2), Data::Boolean(true))]
#[case(Data::UnsignedInt(2), Data::UnsignedInt(2), Data::Boolean(true))]
#[case(Data::UnsignedBigint(2), Data::UnsignedBigint(2), Data::Boolean(true))]
#[case(Data::SignedTinyint(2), Data::SignedTinyint(2), Data::Boolean(true))]
#[case(Data::SignedSmallint(2), Data::SignedSmallint(2), Data::Boolean(true))]
#[case(Data::SignedInt(2), Data::SignedInt(2), Data::Boolean(true))]
#[case(Data::SignedBigint(2), Data::SignedBigint(2), Data::Boolean(true))]
#[case(Data::UnsignedTinyint(0), Data::UnsignedTinyint(2), Data::Boolean(true))]
#[case(Data::UnsignedSmallint(0), Data::UnsignedSmallint(2), Data::Boolean(true))]
#[case(Data::UnsignedInt(0), Data::UnsignedInt(2), Data::Boolean(true))]
#[case(Data::UnsignedBigint(0), Data::UnsignedBigint(2), Data::Boolean(true))]
#[case(Data::SignedTinyint(0), Data::SignedTinyint(2), Data::Boolean(true))]
#[case(Data::SignedSmallint(0), Data::SignedSmallint(2), Data::Boolean(true))]
#[case(Data::SignedInt(0), Data::SignedInt(2), Data::Boolean(true))]
#[case(Data::SignedBigint(0), Data::SignedBigint(2), Data::Boolean(true))]
#[case(Data::UnsignedTinyint(2), Data::UnsignedTinyint(0), Data::Boolean(true))]
#[case(Data::UnsignedSmallint(2), Data::UnsignedSmallint(0), Data::Boolean(true))]
#[case(Data::UnsignedInt(2), Data::UnsignedInt(0), Data::Boolean(true))]
#[case(Data::UnsignedBigint(2), Data::UnsignedBigint(0), Data::Boolean(true))]
#[case(Data::SignedTinyint(2), Data::SignedTinyint(0), Data::Boolean(true))]
#[case(Data::SignedSmallint(2), Data::SignedSmallint(0), Data::Boolean(true))]
#[case(Data::SignedInt(2), Data::SignedInt(0), Data::Boolean(true))]
#[case(Data::SignedBigint(2), Data::SignedBigint(0), Data::Boolean(true))]
#[case(Data::UnsignedTinyint(0), Data::UnsignedTinyint(0), Data::Boolean(false))]
#[case(Data::UnsignedSmallint(0), Data::UnsignedSmallint(0), Data::Boolean(false))]
#[case(Data::UnsignedInt(0), Data::UnsignedInt(0), Data::Boolean(false))]
#[case(Data::UnsignedBigint(0), Data::UnsignedBigint(0), Data::Boolean(false))]
#[case(Data::SignedTinyint(0), Data::SignedTinyint(0), Data::Boolean(false))]
#[case(Data::SignedSmallint(0), Data::SignedSmallint(0), Data::Boolean(false))]
#[case(Data::SignedInt(0), Data::SignedInt(0), Data::Boolean(false))]
#[case(Data::SignedBigint(0), Data::SignedBigint(0), Data::Boolean(false))]
#[case(Data::Boolean(true), Data::Boolean(true), Data::Boolean(true))]
#[case(Data::Boolean(true), Data::Boolean(false), Data::Boolean(true))]
#[case(Data::Boolean(false), Data::Boolean(true), Data::Boolean(true))]
#[case(Data::Boolean(false), Data::Boolean(false), Data::Boolean(false))]
#[case(Data::Varchar("".to_string()), Data::Varchar("".to_string()), Data::Boolean(false))]
#[case(Data::Varchar("a".to_string()), Data::Varchar("a".to_string()), Data::Boolean(true))]
#[case(Data::Text("".to_string()), Data::Text("".to_string()), Data::Boolean(false))]
#[case(Data::Text("a".to_string()), Data::Text("a".to_string()), Data::Boolean(true))]
pub fn test_data_combined_by_or_function(#[case] first: Data, #[case] second: Data, #[case] expected: Data) {
    assert!(first.or(&second) == expected, "{}", format!("{:?} with {:?} should be {:?}", first, second, expected));
}

#[rstest]
#[case(Data::UnsignedTinyint(2), Data::UnsignedTinyint(2), Data::UnsignedTinyint(4))]
#[case(Data::UnsignedSmallint(2), Data::UnsignedSmallint(2), Data::UnsignedSmallint(4))]
#[case(Data::UnsignedInt(2), Data::UnsignedInt(2), Data::UnsignedInt(4))]
#[case(Data::UnsignedBigint(2), Data::UnsignedBigint(2), Data::UnsignedBigint(4))]
#[case(Data::SignedTinyint(2), Data::SignedTinyint(2), Data::SignedTinyint(4))]
#[case(Data::SignedSmallint(2), Data::SignedSmallint(2), Data::SignedSmallint(4))]
#[case(Data::SignedInt(2), Data::SignedInt(2), Data::SignedInt(4))]
#[case(Data::Boolean(true), Data::Boolean(true), Data::Boolean(true))]
#[case(Data::Boolean(true), Data::Boolean(false), Data::Boolean(false))]
#[case(Data::Boolean(false), Data::Boolean(true), Data::Boolean(false))]
#[case(Data::Boolean(false), Data::Boolean(false), Data::Boolean(false))]
#[case(Data::Varchar("a".to_string()), Data::Varchar("a".to_string()), Data::Varchar("aa".to_string()))]
#[case(Data::Text("a".to_string()), Data::Text("a".to_string()), Data::Varchar("aa".to_string()))]
pub fn test_data_combined_by_sum_operator(#[case] first: Data, #[case] second: Data, #[case] expected: Data) {
    assert!(first.clone() + second.clone() == expected, "{}", format!("{:?} with {:?} should be {:?}", first, second, expected));
}

#[rstest]
#[case(Data::Null, Data::Boolean(true))]
#[case(Data::Undefined, Data::Boolean(true))]
#[case(Data::UnsignedTinyint(2), Data::Boolean(false))]
#[case(Data::UnsignedSmallint(2), Data::Boolean(false))]
#[case(Data::UnsignedInt(2), Data::Boolean(false))]
#[case(Data::UnsignedBigint(2), Data::Boolean(false))]
#[case(Data::SignedTinyint(2), Data::Boolean(false))]
#[case(Data::SignedSmallint(2), Data::Boolean(false))]
#[case(Data::SignedInt(2), Data::Boolean(false))]
#[case(Data::SignedBigint(2), Data::Boolean(false))]
#[case(Data::SignedTinyint(-2), Data::Boolean(false))]
#[case(Data::SignedSmallint(-2), Data::Boolean(false))]
#[case(Data::SignedInt(-2), Data::Boolean(false))]
#[case(Data::SignedBigint(-2), Data::Boolean(false))]
#[case(Data::SignedTinyint(0), Data::Boolean(true))]
#[case(Data::SignedSmallint(0), Data::Boolean(true))]
#[case(Data::SignedInt(0), Data::Boolean(true))]
#[case(Data::SignedBigint(0), Data::Boolean(true))]
#[case(Data::UnsignedTinyint(0), Data::Boolean(true))]
#[case(Data::UnsignedSmallint(0), Data::Boolean(true))]
#[case(Data::UnsignedInt(0), Data::Boolean(true))]
#[case(Data::UnsignedBigint(0), Data::Boolean(true))]
#[case(Data::SignedTinyint(0), Data::Boolean(true))]
#[case(Data::SignedSmallint(0), Data::Boolean(true))]
#[case(Data::SignedInt(0), Data::Boolean(true))]
#[case(Data::SignedBigint(0), Data::Boolean(true))]
#[case(Data::Boolean(true), Data::Boolean(false))]
#[case(Data::Boolean(false), Data::Boolean(true))]
#[case(Data::Varchar("".to_string()), Data::Boolean(true))]
#[case(Data::Varchar("a".to_string()), Data::Boolean(false))]
#[case(Data::Text("".to_string()), Data::Boolean(true))]
#[case(Data::Text("a".to_string()), Data::Boolean(false))]
pub fn test_data_not_operator(#[case] first: Data, #[case] expected: Data) {
    assert!(!first.clone() == expected, "{}", format!("{:?} should be {:?}", first, expected));
}

#[rstest]
#[case(Data::Null, Data::Boolean(true))]
#[case(Data::Undefined, Data::Boolean(true))]
#[case(Data::UnsignedTinyint(2), Data::SignedTinyint(-2))]
#[case(Data::UnsignedSmallint(2), Data::SignedSmallint(-2))]
#[case(Data::UnsignedInt(2), Data::SignedInt(-2))]
#[case(Data::UnsignedBigint(2), Data::SignedBigint(-2))]
#[case(Data::SignedTinyint(2), Data::SignedTinyint(-2))]
#[case(Data::SignedSmallint(2), Data::SignedSmallint(-2))]
#[case(Data::SignedInt(2), Data::SignedInt(-2))]
#[case(Data::SignedBigint(2), Data::SignedBigint(-2))]
#[case(Data::SignedTinyint(-2), Data::SignedTinyint(2))]
#[case(Data::SignedSmallint(-2), Data::SignedSmallint(2))]
#[case(Data::SignedInt(-2), Data::SignedInt(2))]
#[case(Data::SignedBigint(-2), Data::SignedBigint(2))]
#[case(Data::SignedTinyint(0), Data::SignedTinyint(0))]
#[case(Data::SignedSmallint(0), Data::SignedSmallint(0))]
#[case(Data::SignedInt(0), Data::SignedInt(0))]
#[case(Data::SignedBigint(0), Data::SignedBigint(0))]
#[case(Data::UnsignedTinyint(0), Data::UnsignedTinyint(0))]
#[case(Data::UnsignedSmallint(0), Data::UnsignedSmallint(0))]
#[case(Data::UnsignedInt(0), Data::UnsignedInt(0))]
#[case(Data::UnsignedBigint(0), Data::UnsignedBigint(0))]
#[case(Data::Boolean(true), Data::Boolean(false))]
#[case(Data::Boolean(false), Data::Boolean(true))]
#[case(Data::Varchar("a".to_string()), Data::Varchar("a".to_string()))]
#[case(Data::Text("a".to_string()), Data::Text("a".to_string()))]
pub fn test_data_neg_operator(#[case] first: Data, #[case] expected: Data) {
    assert!(-first.clone() == expected, "{}", format!("{:?} should be {:?}", first, expected));
}















