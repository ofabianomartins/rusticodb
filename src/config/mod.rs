use std::env;

mod sys_db;

pub use self::sys_db::SysDb;

#[derive(Debug)]
pub struct Config {}

impl Config {

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
