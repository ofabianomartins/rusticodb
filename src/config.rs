use std::env;

#[derive(Debug)]
pub struct Config {

}

impl Config {

    pub fn system_database() -> String {
        return String::from("rusticodb");
    }

    pub fn system_database_table_databases() -> String {
        return String::from("databases");
    }

    pub fn system_database_table_tables() -> String {
        return String::from("tables");
    }

    pub fn system_database_table_columns() -> String {
        return String::from("columns");
    }

    pub fn data_folder() -> String {
        let default_value = "/etc/rusticodb/data";

        return env::var("DATA_FOLDER").unwrap_or_else(|_| default_value.to_string());
    }

}

