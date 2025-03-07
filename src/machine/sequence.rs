use std::fmt;

use crate::config::Config;
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
            Config::sysdb(),
            Config::sysdb_table_sequences(),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true,
            String::from("")
        )
    ];
    data.append(&mut get_sequences_table_definition_without_id());
    return data;
}

pub fn get_sequences_table_definition_without_id() -> Vec<Column> {
    return vec![
        Column::new(
            Config::sysdb(),
            Config::sysdb_table_sequences(),
            String::from("database_name"),
            ColumnType::Varchar,
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            Config::sysdb(),
            Config::sysdb_table_sequences(),
            String::from("table_name"),
            ColumnType::Varchar,
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            Config::sysdb(),
            Config::sysdb_table_sequences(),
            String::from("column_name"),
            ColumnType::Varchar,
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            Config::sysdb(),
            Config::sysdb_table_sequences(),
            String::from("name"),
            ColumnType::Varchar,
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            Config::sysdb(),
            Config::sysdb_table_sequences(),
            String::from("next_id"),
            ColumnType::UnsignedBigint,
            true,
            false,
            false,
            String::from("")
        )
    ];
}
