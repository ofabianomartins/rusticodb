use std::collections::HashMap;

use crate::storage::Tuple;
use crate::storage::Header;
use crate::storage::header_new;
use crate::storage::header_serialize;
use crate::storage::header_deserialize;

use crate::storage::Page;
use crate::storage::page_insert_tuples;
use crate::storage::page_update_tuples;
use crate::storage::page_read_tuples;
use crate::storage::page_new;
use crate::storage::page_amount_left;
use crate::storage::page_serialize;
use crate::storage::page_deserialize;
use crate::storage::tuple_size;


use crate::storage::write_data;
use crate::storage::read_data;
use crate::storage::path_exists;

use crate::utils::Logger;

pub type PagerItem = HashMap<usize, Page>;

pub fn pager_new_pager_item() -> HashMap<usize, Page> {
    return HashMap::new();
}

pub fn pager_insert_pager_item_tuples(pager_item: &mut PagerItem, header: &mut Header, tuples: &mut Vec<Tuple>) {
    let page_count = header.page_count;

    if page_count == 0 {
       let mut page = page_new();

       page_insert_tuples(&mut page, tuples);
       pager_item.insert(1, page);
       header.page_count = 1;
    } else {
        let mut set_new_page = false;
        let tuple_data_size = tuples.iter().map(|item| tuple_size(item) as u64).sum::<u64>();

        pager_item.entry(page_count as usize).and_modify(|_| {}).or_insert(page_new());

        pager_item.entry(page_count as usize)
            .and_modify(|page| {
                if page_amount_left(page) as u64 > tuple_data_size {
                    page_insert_tuples(page, tuples);
                } else {
                    set_new_page = true;
                } 
            })
            .or_insert(page_new());

        if set_new_page == true {
            let mut page = page_new();

            page_insert_tuples(&mut page, tuples);
            pager_item.insert(page_count as usize + 1, page);
            header.page_count += 1;
        }
    }
}

pub fn pager_update_pager_item_tuples(pager_item: &mut PagerItem, tuples: &mut Vec<Tuple>) {
    let pager_item_len = pager_item.len();
    pager_item.entry(pager_item_len).and_modify(|_| {}).or_insert(page_new());

    pager_item.entry(pager_item_len)
        .and_modify(|page| {
            page_update_tuples(page, tuples);
        })
        .or_insert(page_new());
}

pub fn pager_read_pager_item_tuples(pager_item: &mut PagerItem, page_key: &String, header: &mut Header) -> Vec<Tuple> {
    let mut tuples: Vec<Tuple> = Vec::new();
    let page_count = header.page_count;

    if page_count != 0 {
        for page_idx in 1..(page_count + 1) {
            if let Some(page) = pager_item.get(&(page_idx as usize)) {
                tuples.append(&mut page_read_tuples(page))
            } else {
                let buffer: Page = page_deserialize(read_data(page_key, page_idx as u64));
                tuples.append(&mut page_read_tuples(&buffer));
                pager_item.insert(page_idx as usize, buffer);
            }
        }
    }

    return tuples;
}

#[derive(Debug)]
pub struct Pager { 
    pub headers: HashMap<String, Header>,
    pub pages: HashMap<String, PagerItem>
}

impl Pager {
    pub fn new() -> Self {
        Self { headers: HashMap::new(), pages: HashMap::new() }
    }
}

pub fn pager_new() -> Pager {
    return Pager::new()
}

pub fn pager_get_next_rowid(pager: &mut Pager, page_key: &String) -> u64 {
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
            next_rowid = header.next_rowid;
            header.next_rowid += 1;
        })
        .or_insert(header_new());

    return next_rowid;
}

pub fn pager_read_tuples(pager: &mut Pager, page_key: &String) -> Vec<Tuple> {
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
            pager.pages.entry(page_key.clone()).and_modify(|_| {}).or_insert(pager_new_pager_item());

            pager.pages.entry(page_key.clone())
                .and_modify(|pager_item| {
                    tuples.append(&mut pager_read_pager_item_tuples(pager_item, page_key, header));
                })
                .or_insert(pager_new_pager_item());
        })
        .or_insert(header_new());

    return tuples;
}

pub fn pager_insert_tuples(pager: &mut Pager, page_key: &String, tuples: &mut Vec<Tuple>) {
    pager.headers.entry(page_key.clone()).and_modify(|_| {}).or_insert(header_new());

    pager.headers.entry(page_key.clone())
        .and_modify(|header| {
            pager.pages.entry(page_key.clone()).and_modify(|_| {}).or_insert(pager_new_pager_item());

            pager.pages.entry(page_key.clone())
                .and_modify(|pager_item| {
                    pager_insert_pager_item_tuples(pager_item, header, tuples)
                })
                .or_insert(pager_new_pager_item());
        })
        .or_insert(header_new());
}

pub fn pager_update_tuples(pager: &mut Pager, page_key: &String, tuples: &mut Vec<Tuple>) {
    pager.headers.entry(page_key.clone()).and_modify(|_| {}).or_insert(header_new());

    pager.headers.entry(page_key.clone())
        .and_modify(|_header| {
            pager.pages.entry(page_key.clone())
                .and_modify(|pager_item| {
                    pager_update_pager_item_tuples(pager_item, tuples)
                })
                .or_insert(pager_new_pager_item());
        })
        .or_insert(header_new());
}

pub fn pager_flush_page(pager: &mut Pager, page_key: &String) {
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


