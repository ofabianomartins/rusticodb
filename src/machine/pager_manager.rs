use std::collections::HashMap;

use crate::storage::Tuple;
use crate::storage::Header;
use crate::storage::header_new;
use crate::storage::header_serialize;
use crate::storage::header_deserialize;
use crate::storage::header_get_next_rowid;
use crate::storage::pager_insert_tuples;
use crate::storage::pager_update_tuples;
use crate::storage::pager_read_tuples;
use crate::storage::pager_new;
use crate::storage::page_serialize;
use crate::storage::Pager;

use crate::storage::write_data;
use crate::storage::read_data;
use crate::storage::path_exists;

use crate::utils::Logger;

#[derive(Debug)]
pub struct PagerManager { 
    pub headers: HashMap<String, Header>,
    pub pages: HashMap<String, Pager>
}

impl PagerManager {
    pub fn new() -> Self {
        Self { headers: HashMap::new(), pages: HashMap::new() }
    }
}

pub fn pager_manager_new() -> PagerManager {
    return PagerManager::new()
}

pub fn pager_manager_read_tuples(pager: &mut PagerManager, page_key: &String) -> Vec<Tuple> {
    Logger::debug(format!("search page {} on pager", page_key).leak());
    let mut tuples = Vec::new();

    if path_exists(page_key) {
        if let None = pager.headers.get(page_key) {
            pager.headers.insert(page_key.clone(), header_deserialize(&read_data(page_key, 0)));
        }
    }

    pager.headers.entry(page_key.clone()).and_modify(|_| {}).or_insert(header_new());

    pager.headers.entry(page_key.clone())
        .and_modify(|header| {
            pager.pages.entry(page_key.clone()).and_modify(|_| {}).or_insert(pager_new());

            pager.pages.entry(page_key.clone())
                .and_modify(|pager_item| {
                    tuples.append(&mut pager_read_tuples(pager_item, page_key, header));
                })
                .or_insert(pager_new());
        })
        .or_insert(header_new());

    return tuples;
}

pub fn pager_manager_insert_tuples(pager: &mut PagerManager, page_key: &String, tuples: &mut Vec<Tuple>) {
    pager.headers.entry(page_key.clone()).and_modify(|_| {}).or_insert(header_new());

    pager.headers.entry(page_key.clone())
        .and_modify(|header| {
            pager.pages.entry(page_key.clone()).and_modify(|_| {}).or_insert(pager_new());

            pager.pages.entry(page_key.clone())
                .and_modify(|pager_item| {
                    pager_insert_tuples(pager_item, header, tuples)
                })
                .or_insert(pager_new());
        })
        .or_insert(header_new());
}

pub fn pager_manager_update_tuples(pager: &mut PagerManager, page_key: &String, tuples: &mut Vec<Tuple>) {
    pager.headers.entry(page_key.clone()).and_modify(|_| {}).or_insert(header_new());

    pager.headers.entry(page_key.clone())
        .and_modify(|_header| {
            pager.pages.entry(page_key.clone())
                .and_modify(|pager_item| {
                    pager_update_tuples(pager_item, tuples)
                })
                .or_insert(pager_new());
        })
        .or_insert(header_new());
}

pub fn pager_manager_get_next_rowid(pager: &mut PagerManager, page_key: &String) -> u64 {
    Logger::debug(format!("search page {} on pager", page_key).leak());
    let mut next_rowid = 0;

    if path_exists(page_key) {
        if let None = pager.headers.get(page_key) {
            pager.headers.insert(page_key.clone(), header_deserialize(&read_data(page_key, 0)));
        }
    }

    pager.headers.entry(page_key.clone()).and_modify(|_| {}).or_insert(header_new());

    pager.headers.entry(page_key.clone())
        .and_modify(|header| {
            next_rowid = header_get_next_rowid(header);
        })
        .or_insert(header_new());

    return next_rowid;
}


pub fn pager_manager_flush_page(pager: &mut PagerManager, page_key: &String) {
    Logger::debug(format!("FLUSH {}", page_key).leak());
    if let Some(header) = &pager.headers.get(page_key) {
        write_data(page_key, 0, &header_serialize(header));
    }
    if let Some(pager_item) = &pager.pages.get(page_key) {
        for (idx, page) in *pager_item {
            write_data(page_key, *idx as u64, &page_serialize(page));
        }
    }
}



