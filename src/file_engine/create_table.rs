use sqlparser::ast::CreateTable;

use std::fs::File;

pub fn create_table(filepath: &String, statement: CreateTable) {
    let _ = File::create(filepath);
}
