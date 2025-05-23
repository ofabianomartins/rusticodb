use rusticodb::storage::Tuple;
use rusticodb::storage::Page;
use rusticodb::storage::btree_new;
use rusticodb::storage::btree_insert;
use rusticodb::storage::btree_flush;
use rusticodb::storage::read_data;
use rusticodb::storage::page_serialize;
use rusticodb::storage::page_deserialize;
use rusticodb::storage::BLOCK_SIZE;
use rusticodb::storage::Data;
use rusticodb::storage::tuple_new;

#[test]
pub fn test_a_new_btree() {
    let btree = btree_new();
}

/*
#[test]
pub fn test_insert_tuple_with_tinyint() {
    let mut tuple = tuple_new();
    tuple.push(Data::UnsignedTinyint(2u8));

    let btree = btree_new();
    btree_insert(&btree, tuple);
    btree_flush(&btree);

    let page: Page = page_deserialize(read_data(page_key, 0u64));

    let mut buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
    buffer[1] = 1u8;
    buffer[2] = 15u8;
    buffer[3] = 253u8;
    buffer[BLOCK_SIZE-3] = 1u8;
    buffer[BLOCK_SIZE-2] = 4;
    buffer[BLOCK_SIZE-1] = 2u8;

    assert_eq!(page_serialize(&page), buffer);
}
*/
