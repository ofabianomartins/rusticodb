use std::collections::HashMap;

use crate::storage::Tuple;
use crate::storage::Header;
use crate::storage::Page;
use crate::storage::page_insert_tuples;
use crate::storage::page_update_tuples;
use crate::storage::page_read_tuples;
use crate::storage::page_new;
use crate::storage::page_amount_left;
use crate::storage::page_serialize;
use crate::storage::page_deserialize;
use crate::storage::tuple_size;

use crate::storage::read_data;
use crate::storage::write_data;

pub type Pager = HashMap<usize, Page>;

pub fn pager_new() -> HashMap<usize, Page> {
    return HashMap::new();
}

pub fn pager_insert_tuples(pager: &mut Pager, header: &mut Header, tuples: &mut Vec<Tuple>) {
    let page_count = header.page_count;

    if page_count == 0 {
       let mut page = page_new();

       page_insert_tuples(&mut page, tuples);
       pager.insert(1, page);
       header.page_count = 1;
    } else {
        let mut set_new_page = false;
        let tuple_data_size = tuples.iter().map(|item| tuple_size(item) as u64).sum::<u64>();

        pager.entry(page_count as usize).and_modify(|_| {}).or_insert(page_new());

        pager.entry(page_count as usize)
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
            pager.insert(page_count as usize + 1, page);
            header.page_count += 1;
        }
    }
}

pub fn pager_update_tuples(pager: &mut Pager, tuples: &mut Vec<Tuple>) {
    let pager_len = pager.len();
    pager.entry(pager_len).and_modify(|_| {}).or_insert(page_new());

    pager.entry(pager_len)
        .and_modify(|page| {
            page_update_tuples(page, tuples);
        })
        .or_insert(page_new());
}

pub fn pager_read_tuples(pager: &mut Pager, page_key: &String, header: &mut Header) -> Vec<Tuple> {
    let mut tuples: Vec<Tuple> = Vec::new();
    let page_count = header.page_count;

    if page_count != 0 {
        for page_idx in 1..(page_count + 1) {
            if let Some(page) = pager.get(&(page_idx as usize)) {
                tuples.append(&mut page_read_tuples(page))
            } else {
                let buffer: Page = page_deserialize(read_data(page_key, page_idx as u64));
                tuples.append(&mut page_read_tuples(&buffer));
                pager.insert(page_idx as usize, buffer);
            }
        }
    }

    return tuples;
}

pub fn pager_flush_page(pager: &Pager, page_key: &String) {
    for (idx, page) in pager {
        write_data(page_key, *idx as u64, &page_serialize(&page));
    }
}
