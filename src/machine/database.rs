use crate::config::SysDb;

use crate::machine::Column;
use crate::machine::ColumnType;

#[derive(Debug)]
pub struct Database {
    pub id: u64,
    pub name: String
}

impl Database {

    pub fn new(id: u64, name: String) -> Self {
        Database { id, name } 
    }

}

impl PartialEq for Database {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for Database {}

pub fn get_databases_table_definition() -> Vec<Column> {
    let mut data = vec![
        Column::new(
            01u64,
            SysDb::dbname(),
            SysDb::tblname_databases(),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true,
            String::from("")
        )
    ];
    data.append(&mut get_databases_table_definition_without_id());
    return data;
}

pub fn get_databases_table_definition_without_id() -> Vec<Column> {
    return vec![
        Column::new(
            02u64,
            SysDb::dbname(),
            SysDb::tblname_databases(),
            String::from("name"),
            ColumnType::Varchar,
            true,
            false,
            false,
            String::from("")
        )
    ];
}
