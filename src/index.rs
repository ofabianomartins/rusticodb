use std::fmt::Debug;
use serde::{Deserialize, Serialize};

use crate::column::Column;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub name: String,
    pub table: String,
    pub index_type: String,
    pub columns: Vec<Column>
}
