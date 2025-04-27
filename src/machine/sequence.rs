use std::fmt;

use crate::config::SysDb;

use crate::machine::Column;
use crate::machine::ColumnType;

#[derive(Debug, Clone)]
pub struct Sequence {
    pub name: String,
    pub alias: String
}

impl Sequence {

    pub fn new(name: String) -> Self {
        Sequence { name: name.clone(), alias: name }
    }

    pub fn new_with_alias(name: String, alias: String) -> Self {
        Sequence { name, alias }
    }
}

impl PartialEq for Sequence {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for Sequence {}

impl fmt::Display for Sequence {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Sequence({}, {})", self.name, self.alias)
    }
}

pub fn get_sequences_table_definition() -> Vec<Column> {
    let mut data = vec![
        Column::new(
            17u64,
            SysDb::dbname(),
            SysDb::tblname_sequences(),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        )
    ];
    data.append(&mut get_sequences_table_definition_without_id());
    return data;
}

pub fn get_sequences_next_id_column_definition() -> Vec<Column> {
    return vec![
        Column::new(
            22u64,
            SysDb::dbname(),
            SysDb::tblname_sequences(),
            String::from("next_id"),
            ColumnType::UnsignedBigint(0),
            false,
            false,
            false,
            String::from("")
        )
    ];
}

pub fn get_sequences_table_definition_without_id() -> Vec<Column> {
    let mut data = vec![
        Column::new(
            18u64,
            SysDb::dbname(),
            SysDb::tblname_sequences(),
            String::from("database_name"),
            ColumnType::Varchar("".to_string()),
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            19u64,
            SysDb::dbname(),
            SysDb::tblname_sequences(),
            String::from("table_name"),
            ColumnType::Varchar("".to_string()),
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            20u64,
            SysDb::dbname(),
            SysDb::tblname_sequences(),
            String::from("column_name"),
            ColumnType::Varchar("".to_string()),
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            21u64,
            SysDb::dbname(),
            SysDb::tblname_sequences(),
            String::from("name"),
            ColumnType::Varchar("".to_string()),
            true,
            false,
            false,
            String::from("")
        ),
    ];
    data.append(&mut get_sequences_next_id_column_definition());
    return data;
}
