use std::fmt::Debug;

use crate::column::Column;

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>
}
