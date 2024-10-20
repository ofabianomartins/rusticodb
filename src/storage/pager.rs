use std::collections::HashMap;

use crate::config::Config;
use crate::storage::tuple::Tuple;
use crate::storage::page::Page;
use crate::storage::os_interface::OsInterface;
use crate::storage::os_interface::BLOCK_SIZE;

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

    pub fn read_tuples(&mut self, database_name: &String, table_name: &String) -> Vec<Tuple> {
        let page_key = self.format_table_name(database_name, table_name);
        let tuples = Vec::new();

        if let Some(page) = self.pages.get(&page_key) {
            let tuple_count = page.tuple_count();
            let mut tuple_index = 0;

            while tuple_index < tuple_count {

                tuple_index += 1;
            }
        }

        return tuples;
    }


    pub fn write_data(&mut self, database_name: &String, table_name: &String, pos: u64, data: &[u8; BLOCK_SIZE]) {
        OsInterface::write_data(&self.format_table_name(database_name, table_name), pos, data);
    }

    pub fn read_data(&mut self, database_name: &String, table_name: &String, pos: u64) -> [u8; BLOCK_SIZE] {
        return OsInterface::read_data(&self.format_table_name(database_name, table_name), pos);
    }

    pub fn format_database_name(&mut self, database_name: &String) -> String{
        return format!("{}/{}", Config::data_folder(), database_name);
    }

    pub fn format_table_name(&mut self, database_name: &String, table_name: &String) -> String{
        return format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);
    }
}
