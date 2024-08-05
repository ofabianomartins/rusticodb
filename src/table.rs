use std::fmt::Debug;
use serde::{Deserialize, Serialize};

use crate::column::Column;
use crate::index::Index;

pub type Rows = Vec<String>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub indexes: Vec<Index>,
    pub rows: Rows
}

impl Table {
    pub fn append(&mut self, row: String) {
        self.rows.push(row)
    }
}
