
use std::path::Path;

use rusticodb::storage::utils::create_if_not_exists_data_folder;
use rusticodb::storage::config::Config;

use crate::test_utils::destroy_tmp_test_folder;

#[test]
pub fn test_system_database_folder() {
    create_if_not_exists_data_folder();

    let metadata_foldername = format!("{}", Config::data_folder());
    assert!(Path::new(&metadata_foldername).exists());

    destroy_tmp_test_folder();
}

