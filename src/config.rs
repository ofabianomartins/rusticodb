use std::env;

#[derive(Debug)]
pub struct Config {}

impl Config {

    pub fn sysdb() -> String {
        return String::from("rusticodb");
    }

    pub fn sysdb_table_databases() -> String {
        return String::from("databases");
    }

    pub fn sysdb_table_tables() -> String {
        return String::from("tables");
    }

    pub fn sysdb_table_columns() -> String {
        return String::from("columns");
    }

    pub fn sysdb_table_sequences() -> String {
        return String::from("sequences");
    }

    pub fn sysdb_table_indexes() -> String {
        return String::from("indexes");
    }

    pub fn log_mode() -> u8 {
        match env::var("LOG_MODE") {
            Ok(value) => match value.parse::<u8>() {
                Ok(value2) => value2,
                Err(_) => 0u8
            },
            Err(_) => 0u8
        }
    }

    pub fn data_folder() -> String {
        match env::var("DATA_FOLDER") {
            Ok(value) => value.to_string(),
            Err(_) => String::from("/etc/rusticodb/data")
        } 
    }

}
