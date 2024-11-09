use crate::config::Config;

pub struct Logger {
}

impl Logger {
    pub fn info(message: &'static str) {
        if Config::log_mode() < 4 {
            println!("[INFO]: {}", message);
        }
    }

    pub fn warn(message: &'static str) {
        if Config::log_mode() < 3 {
            println!("[WARN]: {}", message);
        }
    }

    pub fn error(message: &'static str) {
        if Config::log_mode() < 2 {
            println!("[ERROR]: {}", message);
        }
    }

    pub fn debug(message: &'static str) {
        if Config::log_mode() < 1 {
            println!("[DEBUG]: {}", message);
        } 
    }
}
