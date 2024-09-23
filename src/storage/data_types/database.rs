use crate::storage::data_types::table::Table;

#[derive(Debug)]
pub struct Database {
    pub name: String,
    pub tables: Vec<Table>
}
