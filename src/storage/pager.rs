use std::collections::HashMap;

use crate::storage::Tuple;
use crate::storage::Page;
use crate::storage::page_insert_tuples;
use crate::storage::page_update_tuples;
use crate::storage::page_read_tuples;
use crate::storage::page_new;
use crate::storage::write_data;
use crate::storage::read_data;

use crate::utils::Logger;

pub type Pager = HashMap<String, Page>;

pub fn pager_read_tuples(pager: &mut Pager, page_key: &String) -> Vec<Tuple> {
    Logger::debug(format!("search page {} on pager", page_key).leak());
    if let Some(page) = pager.get(page_key) {
        Logger::debug(format!("read page {} from pager", page_key).leak());
        return page_read_tuples(page);
    } else {
        Logger::debug(format!("read page {} from file", page_key).leak());
        let page: Page = read_data(page_key, 0u64);
        pager.insert(page_key.clone(), page);

        if let Some(page) = pager.get(page_key) {
            return page_read_tuples(page);
        } 
    }
    return Vec::new();
}

pub fn pager_insert_tuples(pager: &mut Pager, page_key: &String, tuples: &mut Vec<Tuple>) {
    pager.entry(page_key.clone()).and_modify(|_| {}).or_insert(page_new());

    pager.entry(page_key.clone())
        .and_modify(|page| {
            page_insert_tuples(page, tuples);
        })
        .or_insert(page_new());
}


pub fn pager_update_tuples(pager: &mut Pager, page_key: &String, tuples: &mut Vec<Tuple>) {
    pager.entry(page_key.clone()).and_modify(|_| {}).or_insert(page_new());

    pager.entry(page_key.clone())
        .and_modify(|page| {
            page_update_tuples(page, tuples);
        })
        .or_insert(page_new());
}



pub fn pager_flush_page(pager: &mut Pager, page_key: &String) {
    Logger::debug(format!("FLUSH {}", page_key).leak());
    if let Some(page) = &pager.get(page_key) {
        write_data(page_key, 0, &page);
    }
}


