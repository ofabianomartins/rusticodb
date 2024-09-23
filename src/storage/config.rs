use std::env;

#[derive(Debug)]
pub struct Config {

}

impl Config {

    pub fn data_folder() -> String {
        let default_value = "/etc/rusticodb/data";

        return env::var("DATA_FOLDER").unwrap_or_else(|_| default_value.to_string());
    }

}

