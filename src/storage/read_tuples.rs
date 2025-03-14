use crate::storage::Tuple;
use crate::storage::Page;
use crate::storage::Pager;
use crate::storage::read_data;
use crate::storage::page_read_tuples;

use crate::utils::Logger;

pub fn read_tuples(pager: &mut Pager, page_key: &String) -> Vec<Tuple> {
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

