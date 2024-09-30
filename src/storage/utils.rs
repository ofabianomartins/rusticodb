use std::fs::create_dir;

use crate::storage::config::Config;

pub fn create_if_not_exists_data_folder() {
    let server_folder = format!("{}", Config::data_folder());

    let _ = create_dir(server_folder);
}
