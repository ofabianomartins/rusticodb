pub struct Logger {
}


impl Logger {
    pub fn info(message: &'static str) {
        println!("[INFO]: {}", message);
    }

    pub fn warn(message: &'static str) {
        println!("[WARN]: {}", message);
    }

    pub fn error(message: &'static str) {
        println!("[ERROR]: {}", message);
    }

    pub fn debug(message: &'static str) {
        println!("[DEBUG]: {}", message);
    }
}
