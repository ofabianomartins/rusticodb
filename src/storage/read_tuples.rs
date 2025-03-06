use crate::storage::Tuple;
use crate::storage::Page;
use crate::storage::Pager;
use crate::storage::read_data;

pub fn read_tuples(pager: &mut Pager, page_key: &String) -> Vec<Tuple> {
    if let Some(page) = pager.get(page_key) {
        return page.read_tuples();
    } else {
        let data = read_data(page_key, 0u64);
        let page = Page::load(0, data);
        pager.insert(page_key.clone(), page);

        if let Some(page) = pager.get(page_key) {
            return page.read_tuples();
        } 
    }
    return Vec::new();
}

