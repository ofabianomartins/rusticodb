use std::fmt::Debug;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs;
use std::fs::File;
use std::collections::HashMap;

use sqlparser::ast::*;
use sqlparser::ast::CreateTable;
use sqlparser::ast::Insert;

use crate::table::Table;
use crate::table::Rows;
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
                indexes: Vec::new(),
                rows: Vec::new()
            }
        )
    }

    pub fn insert_row(&mut self, insert_statement: Insert ) {
        let index = self.tables.iter()
            .position(|e| e.name == insert_statement.table_name.to_string());

        match index {
            Some(id) => {
                // let row = format!("{0}_{1}", insert_statement.columns.to_vec(), "");
                
                let mut json_content: HashMap<String, String> = HashMap::new();
                if let Some(source) = insert_statement.source {
                    match *source.body {
                        SetExpr::Values(Values { rows, .. }) => {
                            for (index, column) in self.tables[id].columns.iter().enumerate() {
                                println!("Message ::: {}", rows[0][index]);
                                json_content.insert(column.name.to_string(), rows[0][index].clone().to_string());
                            }
                        }
                        _ => unreachable!(),
                    }
                }

                let json_str = serde_json::to_string(&json_content).expect("Wrong format!!!");
                self.tables[id].rows.push(json_str);
            },
            None => {

            }
        }
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
