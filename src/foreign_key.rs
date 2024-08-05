use std::fmt::Debug;

use crate::column::Column;

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub origin_table: String,
    pub origin_column: Column,
    pub destiny_table: String,
    pub destiny_column: Column
}
