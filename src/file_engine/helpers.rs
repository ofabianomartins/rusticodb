use std::fs::File;
use std::fs::create_dir_all;

use crate::config::Config;

pub fn create_base_folder(config: &Config) {
    let _ = create_dir_all(config.base_path.clone());
    let _ = File::create(format!("{0}/data.json", config.base_path.clone()));
}
