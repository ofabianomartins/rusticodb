use std::path::Path;

use rusticodb::config::Config;
use rusticodb::storage::create_folder_if_not_exists;

use crate::test_utils::destroy_tmp_test_folder;

#[test]
pub fn test_system_database_folder() {
    create_folder_if_not_exists(&Config::data_folder());

    let metadata_foldername = format!("{}", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());

    destroy_tmp_test_folder();
}

