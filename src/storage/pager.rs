use std::collections::HashMap;

use crate::storage::Tuple;
use crate::storage::Header;
use crate::storage::header_new;
use crate::storage::header_page_count;
use crate::storage::header_set_page_count;

use crate::storage::Page;
use crate::storage::page_insert_tuples;
use crate::storage::page_update_tuples;
use crate::storage::page_read_tuples;
use crate::storage::page_new;
use crate::storage::page_amount_left;
use crate::storage::write_data;

use crate::utils::Logger;

pub type PagerItem = HashMap<usize, Page>;

pub fn pager_new_pager_item() -> HashMap<usize, Page> {
    return HashMap::new();
}

pub fn pager_insert_pager_item_tuples(pager_item: &mut PagerItem, header: &mut Header, tuples: &mut Vec<Tuple>) {
    let page_count = header_page_count(header);

    if page_count == 0 {
       let mut page = page_new();

       page_insert_tuples(&mut page, tuples);
       pager_item.insert(1, page);
       header_set_page_count(header, 1);
    } else {
        let mut set_new_page = false;
        let tuple_data_size = tuples.iter().map(|item| item.len() as u64).sum::<u64>();

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
            header_set_page_count(header, page_count + 1);
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

pub fn pager_read_pager_item_tuples(pager_item: &mut PagerItem, header: &mut Header) -> Vec<Tuple> {
    let mut tuples: Vec<Tuple> = Vec::new();
    let page_count = header_page_count(header);

    if page_count != 0 {
        for page_idx in 1..(page_count + 1) {
            if let Some(page) = pager_item.get(&(page_idx as usize)) {
                tuples.append(&mut page_read_tuples(page))
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

pub fn pager_read_tuples(pager: &mut Pager, page_key: &String) -> Vec<Tuple> {
    Logger::debug(format!("search page {} on pager", page_key).leak());
    let mut tuples = Vec::new();
    pager.headers.entry(page_key.clone()).and_modify(|_| {}).or_insert(header_new());

    pager.headers.entry(page_key.clone())
        .and_modify(|header| {
            pager.pages.entry(page_key.clone()).and_modify(|_| {}).or_insert(pager_new_pager_item());

            pager.pages.entry(page_key.clone())
                .and_modify(|pager_item| {
                    tuples.append(&mut pager_read_pager_item_tuples(pager_item, header));
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
        write_data(page_key, 0, &header);
    }
    if let Some(pager_item) = &pager.pages.get(page_key) {
        for (idx, page) in *pager_item {
            write_data(page_key, *idx as u64 + 1, &page);
        }
    }
}


