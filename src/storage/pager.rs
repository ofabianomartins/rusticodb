use std::collections::HashMap;

use crate::config::Config;

use crate::storage::Tuple;
use crate::storage::Page;
use crate::storage::OsInterface;
use crate::storage::BLOCK_SIZE;

use crate::utils::Logger;

#[derive(Debug)]
pub struct Pager { 
    pub pages: HashMap<String, Page>
}

impl Pager {
    pub fn new() -> Self {
        Self { pages: HashMap::new() }
    }

    pub fn insert_tuples(&mut self, database_name: &String, table_name: &String, tuples: &mut Vec<Tuple>) {
        let page_key = self.format_table_name(database_name, table_name);

        self.pages.entry(page_key.clone()).and_modify(|_| {}).or_insert(Page::new(0));

        self.pages.entry(page_key.clone())
            .and_modify(|page| {
                page.insert_tuples(tuples);
            })
            .or_insert(Page::new(0));
    }

    pub fn update_tuples(&mut self, database_name: &String, table_name: &String, tuples: &mut Vec<Tuple>) {
        let page_key = self.format_table_name(database_name, table_name);

        self.pages.entry(page_key.clone()).and_modify(|_| {}).or_insert(Page::new(0));

        self.pages.entry(page_key.clone())
            .and_modify(|page| {
                page.update_tuples(tuples);
            })
            .or_insert(Page::new(0));
    }

    pub fn read_tuples(&mut self, database_name: &String, table_name: &String) -> Vec<Tuple> {
        let page_key = self.format_table_name(database_name, table_name);

        if let Some(page) = self.pages.get(&page_key) {
            return page.read_tuples();
        } else {
            let data = self.read_data(database_name, table_name, 0u64);
            let page = Page::load(0, data);
            self.pages.insert(page_key.clone(), page);

            if let Some(page) = self.pages.get(&page_key) {
                return page.read_tuples();
            } 
        }
        return Vec::new();
    }

    pub fn flush_page(&mut self, database_name: &String, table_name: &String) {
        let page_key = self.format_table_name(database_name, table_name);
        Logger::debug(format!("FLUSH database {} table {}", database_name, table_name).leak());
        if let Some(page) = &self.pages.get(&page_key) {
            self.write_data(database_name, table_name, 0, &page.data);
        }
    }

    pub fn write_data(&self, database_name: &String, table_name: &String, pos: u64, data: &[u8; BLOCK_SIZE]) {
        OsInterface::write_data(&self.format_table_name(database_name, table_name), pos, data);
    }

    pub fn read_data(&self, database_name: &String, table_name: &String, pos: u64) -> [u8; BLOCK_SIZE] {

        if OsInterface::path_exists(&self.format_table_name(database_name, table_name)) {
             return OsInterface::read_data(&self.format_table_name(database_name, table_name), pos);
        }
        let mut empty = [0; BLOCK_SIZE];
        empty[3] = 4u8;
        return empty;
    }

    pub fn format_database_name(&self, database_name: &String) -> String{
        return format!("{}/{}", Config::data_folder(), database_name);
    }

    pub fn format_table_name(&self, database_name: &String, table_name: &String) -> String{
        return format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);
    }
}
