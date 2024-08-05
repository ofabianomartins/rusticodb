use std::env;

pub struct Config {
    pub base_path: String
}

impl Config {
    pub fn build() -> Config {
        let base_path: String = match env::var("SYSTEM_BASE_PATH") {
            Ok(value) => value,
            Err(_) => {
                String::from("/tmp/rusticodb/main")
            }
        };

        Config {
            base_path
        }
    }
}
