use std::fs::remove_dir_all;
use std::fs::create_dir_all;

pub fn create_tmp_test_folder() {
    create_dir_all("./tmp_tests");
}

pub fn destroy_tmp_test_folder() {
    remove_dir_all("./tmp_tests");
}
