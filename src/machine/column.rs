use std::fmt;

use crate::storage::Data;

use crate::config::SysDb;

pub type ColumnType = Data;

#[derive(Debug, Clone)]
pub struct Column {
    pub id: u64,
    pub database_name: String,
    pub database_alias: String,
    pub table_name: String,
    pub table_alias: String,
    pub name: String,
    pub alias: String,
    pub column_type: ColumnType,
    pub not_null: bool,
    pub unique: bool,
    pub primary_key: bool,
    pub default: String
}


impl Column {
    pub fn new(
        id: u64,
        database_name: String,
        table_name: String,
        name: String,
        column_type: ColumnType,
        not_null: bool,
        unique: bool,
        primary_key: bool,
        default: String
    ) -> Self {
        Column { 
            id,
            database_name: database_name.clone(), 
            database_alias: database_name,
            table_name: table_name.clone(), 
            table_alias: table_name, 
            name: name.clone(), 
            alias: name, 
            column_type,
            not_null,
            unique,
            primary_key,
            default
        }
    }

    pub fn new_with_alias(
        id: u64,
        database_name: String,
        database_alias: String,
        table_name: String,
        table_alias: String,
        name: String,
        alias: String,
        column_type: ColumnType,
        not_null: bool,
        unique: bool,
        primary_key: bool,
        default: String
    ) -> Self {
        Column { 
            id,
            database_name, 
            database_alias, 
            table_name, 
            table_alias, 
            name, 
            alias, 
            column_type,
            not_null,
            unique,
            primary_key,
            default
        }
    }

    pub fn get_type_column(self) -> String {
        return match self.column_type {
            ColumnType::UnsignedTinyint(_) => String::from("UNSIGNED TINYINT"),
            ColumnType::SignedTinyint(_) => String::from("SIGNED TINYINT"),
            ColumnType::UnsignedSmallint(_) => String::from("UNSIGNED SMALLINT"),
            ColumnType::SignedSmallint(_) => String::from("SIGNED SMALLINT"),
            ColumnType::UnsignedInt(_) => String::from("UNSIGNED INT"),
            ColumnType::SignedInt(_) => String::from("SIGNED INT"),
            ColumnType::UnsignedBigint(_) => String::from("UNSIGNED BIGINT"),
            ColumnType::SignedBigint(_) => String::from("SIGNED BIGINT"),
            ColumnType::Varchar(_) => String::from("VARCHAR"),
            ColumnType::Text(_) => String::from("TEXT"),
            ColumnType::Boolean(_) => String::from("UNSIGNED TINYINT"),
            _ => String::from("UNDEFINED")
        };
    }

    pub fn check_column_name(&self, other_name: &String) -> bool {
        return self.name == *other_name; 
    }

    pub fn is_number(self) -> bool {
        return matches!(
            self.column_type,
            ColumnType::UnsignedTinyint(_) |
            ColumnType::UnsignedSmallint(_) |
            ColumnType::UnsignedInt(_) |
            ColumnType::UnsignedBigint(_) |
            ColumnType::SignedTinyint(_) |
            ColumnType::SignedSmallint(_) |
            ColumnType::SignedInt(_) |
            ColumnType::SignedBigint(_)
        );
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        // println!("\n\n\n{}\n{}\n\n\n", self, other);
        (self.name == other.name && self.table_name == other.table_name) ||
        (self.alias == other.alias && self.table_alias == other.table_alias)
    }
}
impl Eq for Column {}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} {} {} {} {} {}",
            self.database_name,
            self.database_alias,
            self.table_alias,
            self.table_name,
            self.name,
            self.alias
        )
    }
}


pub fn map_column_type(value: String) -> ColumnType {
    match value.as_str() {
        "UNSIGNED TINYINT" => ColumnType::UnsignedTinyint(0),
        "SIGNED TINYINT" => ColumnType::SignedTinyint(0),
        "UNSIGNED SMALLINT" => ColumnType::UnsignedSmallint(0),
        "SIGNED SMALLINT" => ColumnType::SignedSmallint(0),
        "UNSIGNED INT" => ColumnType::UnsignedInt(0),
        "SIGNED INT" => ColumnType::SignedInt(0),
        "UNSIGNED BIGINT" => ColumnType::UnsignedBigint(0),
        "SIGNED BIGINT" => ColumnType::SignedBigint(0),
        "VARCHAR" => ColumnType::Varchar("".to_string()),
        "TEXT" => ColumnType::Text("".to_string()),
        _ => ColumnType::Varchar("".to_string())
    }
}

pub fn get_columns_table_definition() -> Vec<Column> {
    let mut data = vec![
        Column::new(
            08u64,
            SysDb::dbname(),
            SysDb::tblname_columns(),
            String::from("id"),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            String::from("")
        )
    ];
    data.append(&mut get_columns_table_definition_without_id());
    return data;
}

pub fn get_columns_table_definition_without_id() -> Vec<Column> {
    return vec![
        Column::new(
            09u64,
            SysDb::dbname(),
            SysDb::tblname_columns(),
            String::from("database_name"),
            ColumnType::Varchar("".to_string()),
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            10u64,
            SysDb::dbname(),
            SysDb::tblname_columns(),
            String::from("table_name"),
            ColumnType::Varchar("".to_string()),
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            11u64,
            SysDb::dbname(),
            SysDb::tblname_columns(),
            String::from("name"),
            ColumnType::Varchar("".to_string()),
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            12u64,
            SysDb::dbname(),
            SysDb::tblname_columns(),
            String::from("type"),
            ColumnType::Varchar("".to_string()),
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            13u64,
            SysDb::dbname(),
            SysDb::tblname_columns(),
            String::from("not_null"),
            ColumnType::Boolean(true),
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            14u64,
            SysDb::dbname(),
            SysDb::tblname_columns(),
            String::from("unique"),
            ColumnType::Boolean(true),
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            15u64,
            SysDb::dbname(),
            SysDb::tblname_columns(),
            String::from("primary_key"),
            ColumnType::Boolean(true),
            true,
            false,
            false,
            String::from("")
        ),
        Column::new(
            16u64,
            SysDb::dbname(),
            SysDb::tblname_columns(),
            String::from("default"),
            ColumnType::Varchar("".to_string()),
            true,
            false,
            false,
            String::from("")
        )
    ];
}
