use crate::storage::Pager;
use crate::storage::write_data;

use crate::utils::Logger;

pub fn flush_page(pager: &mut Pager, page_key: &String) {
    Logger::debug(format!("FLUSH {}", page_key).leak());
    if let Some(page) = &pager.get(page_key) {
        write_data(page_key, 0, &page.data);
    }
}

