use sqlparser::ast::CreateTable;

use std::fs::File;
use std::fs::create_dir_all;

use crate::config::Config;

pub fn create_table(config: &Config, statement: CreateTable) {
    let table_path = format!("{0}/tables/{1}", config.base_path, statement.name);

    let _ = create_dir_all(table_path.clone());
    let _ = File::create(format!("{table_path}/metadata.json"));
}
