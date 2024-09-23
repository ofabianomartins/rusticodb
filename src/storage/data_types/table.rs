
use crate::storage::data_types::column::Column;

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>
}
