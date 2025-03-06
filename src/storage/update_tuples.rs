use crate::storage::Tuple;
use crate::storage::Page;
use crate::storage::Pager;

pub fn update_tuples(pager: &mut Pager, page_key: &String, tuples: &mut Vec<Tuple>) {
    pager.entry(page_key.clone()).and_modify(|_| {}).or_insert(Page::new(0));

    pager.entry(page_key.clone())
        .and_modify(|page| {
            page.update_tuples(tuples);
        })
        .or_insert(Page::new(0));
}

