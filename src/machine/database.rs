use crate::config::Config;
use crate::machine::Column;
use crate::machine::ColumnType;

#[derive(Debug)]
pub struct Database {
    name: String
}

impl Database {

    pub fn new(name: String) -> Self {
        Database { name } 
    }

    pub fn check_name(&self, other_name: &String) -> bool {
        return self.name == *other_name; 
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
            Config::sysdb(),
            Config::sysdb_table_databases(),
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
            Config::sysdb(),
            Config::sysdb_table_databases(),
            String::from("name"),
            ColumnType::Varchar,
            true,
            false,
            false,
            String::from("")
        )
    ];
}
