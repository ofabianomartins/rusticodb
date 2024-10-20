use rusticodb::storage::tuple::Tuple;
use rusticodb::storage::page::Page;

#[test]
pub fn test2_insert_two_tuples_on_pager_and_read_both() {
    let data: String = String::from("simple_string");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple = Tuple::new();
    tuple.push_string(&data);
    tuples.push(tuple);

    let mut tuples2: Vec<Tuple> = Vec::new();
    let mut tuple = Tuple::new();
    tuple.push_string(&data);
    tuples.push(tuple);

    let mut page = Page::new(0);
    page.insert_tuples(&mut tuples);
    page.insert_tuples(&mut tuples2);

    let tuples = page.read_tuples();

    assert_eq!(tuples.len(), 2);
    assert_eq!(tuples[0].cell_count(), 1);
    assert_eq!(tuples[1].cell_count(), 1);
}
