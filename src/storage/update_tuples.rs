use crate::storage::Tuple;
use crate::storage::Pager;
use crate::storage::page_new;
use crate::storage::page_update_tuples;

pub fn update_tuples(pager: &mut Pager, page_key: &String, tuples: &mut Vec<Tuple>) {
    pager.entry(page_key.clone()).and_modify(|_| {}).or_insert(page_new(0));

    pager.entry(page_key.clone())
        .and_modify(|page| {
            page_update_tuples(page, tuples);
        })
        .or_insert(page_new(0));
}

