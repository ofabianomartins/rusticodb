use rusticodb::storage::format_table_name;
use rusticodb::storage::format_database_name;
use rusticodb::storage::Pager;
use rusticodb::storage::tuple_new;
use rusticodb::storage::pager_insert_tuples;
use rusticodb::storage::pager_read_tuples;
use rusticodb::storage::pager_flush_page;
use rusticodb::storage::create_file;
use rusticodb::storage::create_folder;
use rusticodb::storage::Data;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_write_data_100_tuples() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let page_key = format_table_name(&database1, &table1);

    create_tmp_test_folder();

    let mut pager = Pager::new();

    for _ in 1..100 {
        let mut tuple = tuple_new();
        tuple.push(Data::UnsignedTinyint(2u8));

        pager_insert_tuples(&mut pager, &page_key, &mut vec![tuple]);
    }

    assert!(matches!(pager.headers.get(&page_key), Some(_hash_page)));
    assert!(matches!(pager.pages.get(&page_key), Some(_hash_page)));
    assert!(matches!(pager.pages.get(&page_key).unwrap().get(&1), Some(_page)));
}

#[test]
pub fn test_read_data_metadata_file() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let page_key = format_table_name(&database1, &table1);

    create_tmp_test_folder();

    let mut pager = Pager::new();

    for _ in 0..100 {
        let mut tuple = tuple_new();
        tuple.push(Data::UnsignedBigint(2u64));
        tuple.push(Data::UnsignedBigint(3u64));
        tuple.push(Data::UnsignedBigint(4u64));
        tuple.push(Data::UnsignedBigint(5u64));
        tuple.push(Data::UnsignedBigint(6u64));

        pager_insert_tuples(&mut pager, &page_key, &mut vec![tuple]);
    }

    let tuples = pager_read_tuples(&mut pager, &page_key);

    assert_eq!(tuples.len(), 100);
    assert!(matches!(pager.headers.get(&page_key), Some(_hash_page)));
    assert!(matches!(pager.pages.get(&page_key), Some(_hash_page)));
    assert!(matches!(pager.pages.get(&page_key).unwrap().get(&1), Some(_page)));
    assert!(matches!(pager.pages.get(&page_key).unwrap().get(&2), Some(_page)));
}

#[test]
pub fn test_read_data_from_new_pager() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let database_key = format_database_name(&database1);
    let page_key = format_table_name(&database1, &table1);

    create_tmp_test_folder();

    create_folder(&database_key);
    create_file(&page_key);

    let mut pager = Pager::new();

    for _ in 0..100 {
        let mut tuple = tuple_new();
        tuple.push(Data::UnsignedBigint(2u64));
        tuple.push(Data::UnsignedBigint(3u64));
        tuple.push(Data::UnsignedBigint(4u64));
        tuple.push(Data::UnsignedBigint(5u64));
        tuple.push(Data::UnsignedBigint(6u64));

        pager_insert_tuples(&mut pager, &page_key, &mut vec![tuple]);
    }

    pager_flush_page(&mut pager, &page_key);

    let mut pager_new = Pager::new();

    let tuples = pager_read_tuples(&mut pager_new, &page_key);

    assert_eq!(tuples.len(), 100);
    assert!(matches!(pager.headers.get(&page_key), Some(_hash_page)));
    assert!(matches!(pager.pages.get(&page_key), Some(_hash_page)));
    assert!(matches!(pager.pages.get(&page_key).unwrap().get(&1), Some(_page)));
    assert!(matches!(pager.pages.get(&page_key).unwrap().get(&2), Some(_page)));
}
