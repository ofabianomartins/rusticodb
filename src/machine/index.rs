use std::fmt;

use crate::config::SysDb;

use crate::machine::Column;
use crate::machine::ColumnType;

#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub alias: String
}

impl Index {

    pub fn new(name: String) -> Self {
        Index { name: name.clone(), alias: name }
    }

    pub fn new_with_alias(name: String, alias: String) -> Self {
        Index { name, alias }
    }
}

impl PartialEq for Index {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for Index {}

impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Index({}, {})", self.name, self.alias)
    }
}

pub fn get_indexes_table_definition() -> Vec<Column> {
    let mut data = vec![
        Column::new(
            SysDb::dbname(),
            SysDb::tblname_indexes(),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true,
            String::from("")
        )
    ];
    data.append(&mut get_indexes_table_definition_without_id());
    return data;
}

pub fn get_indexes_table_definition_without_id() -> Vec<Column> {
    return vec![
        Column::new(
            SysDb::dbname(),
            SysDb::tblname_indexes(),
            String::from("database_name"),
            ColumnType::Varchar,
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            SysDb::dbname(),
            SysDb::tblname_indexes(),
            String::from("table_name"),
            ColumnType::Varchar,
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            SysDb::dbname(),
            SysDb::tblname_indexes(),
            String::from("column_name"),
            ColumnType::Varchar,
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            SysDb::dbname(),
            SysDb::tblname_indexes(),
            String::from("name"),
            ColumnType::Varchar,
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            SysDb::dbname(),
            SysDb::tblname_indexes(),
            String::from("type"),
            ColumnType::Varchar,
            true,
            false,
            false,
            String::from("")
        )
    ];
}
