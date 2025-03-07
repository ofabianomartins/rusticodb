use crate::storage::Tuple;
use crate::storage::Page;
use crate::storage::Pager;
use crate::storage::read_data;

use crate::utils::Logger;

pub fn read_tuples(pager: &mut Pager, page_key: &String) -> Vec<Tuple> {
    Logger::debug(format!("search page {} on pager", page_key).leak());
    if let Some(page) = pager.get(page_key) {
        Logger::debug(format!("read page {} from pager", page_key).leak());
        return page.read_tuples();
    } else {
        Logger::debug(format!("read page {} from file", page_key).leak());
        let data = read_data(page_key, 0u64);
        let page = Page::load(0, data);
        pager.insert(page_key.clone(), page);

        if let Some(page) = pager.get(page_key) {
            return page.read_tuples();
        } 
    }
    return Vec::new();
}

