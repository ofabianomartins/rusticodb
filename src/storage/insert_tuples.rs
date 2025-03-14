use crate::storage::Tuple;
use crate::storage::Pager;
use crate::storage::page_insert_tuples;
use crate::storage::page_new;

pub fn insert_tuples(pager: &mut Pager, page_key: &String, tuples: &mut Vec<Tuple>) {
    pager.entry(page_key.clone()).and_modify(|_| {}).or_insert(page_new(0));

    pager.entry(page_key.clone())
        .and_modify(|page| {
            page_insert_tuples(page, tuples);
        })
        .or_insert(page_new(0));
}

