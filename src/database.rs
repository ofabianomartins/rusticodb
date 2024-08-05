use std::fmt::Debug;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;

use crate::table::Table;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub tables: Vec<Table>
}

impl Database {
    pub fn read_database(filepath: &String) -> Database {
        match File::open(filepath) {
            Ok(file) => {
                let database: Database = serde_json::from_reader(&file).unwrap();
                database
            },
            Err(error) => {
                Database { tables: Vec::new() }
            }
        }
        
    }

    pub fn write_database(&self, filepath: &String) {
        let json: String = serde_json::to_string(&self).unwrap();

        fs::write(filepath, json);
    }
}
