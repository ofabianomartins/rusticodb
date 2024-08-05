use std::fmt::Debug;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;

use sqlparser::ast::CreateTable;

use crate::table::Table;
use crate::column::Column;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub tables: Vec<Table>
}

impl Database {

    pub fn append_table(&mut self, create_table_statement: CreateTable ) {
        for table in self.tables.clone() {
            if table.name == create_table_statement.name.to_string() {
                return;
            }
        }

        let mut table_columns: Vec<Column> = Vec::new();

        for column in create_table_statement.columns {
            table_columns.push(
                Column {
                    name: column.name.to_string(),
                    data_type: column.data_type.to_string()
                }
            );

        }

        self.tables.push(
            Table {
                name: create_table_statement.name.to_string(),
                columns: table_columns,
                indexes: Vec::new()
            }
        )
    }


    pub fn read_database(filepath: &String) -> Database {
        match File::open(filepath) {
            Ok(file) => {
                let database: Database = serde_json::from_reader(&file).unwrap();
                database
            },
            Err(_) => {
                Database { tables: Vec::new() }
            }
        }
    }

    pub fn write_database(&self, filepath: &String) {
        let json: String = serde_json::to_string(&self).unwrap();

        fs::write(filepath, json);
    }
}
