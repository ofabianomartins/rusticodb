use crate::storage::pager::Pager;
use crate::storage::utils::create_if_not_exists_data_folder;

pub fn setup_system() {
    create_if_not_exists_data_folder();

    let mut pager = Pager::new();

    let main_db_name = String::from("rusticodb");

    pager.create_database(&main_db_name);
    pager.create_file(&main_db_name, &String::from("databases"));
    pager.create_file(&main_db_name, &String::from("tables"));
    pager.create_file(&main_db_name, &String::from("columns"));
}
