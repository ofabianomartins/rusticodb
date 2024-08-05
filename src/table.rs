use std::fmt::Debug;
use serde::{Deserialize, Serialize};

use crate::column::Column;
use crate::index::Index;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub indexes: Vec<Index>
}
