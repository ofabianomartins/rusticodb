use crate::parser::sql_executor::SqlExecutor;
use crate::storage::utils::create_if_not_exists_data_folder;

pub fn setup_system() {
    create_if_not_exists_data_folder();

    let mut executor = SqlExecutor::new();

    executor.parse_command("CREATE DATABASE rusticodb");
    executor.parse_command("USE rusticodb");
    executor.parse_command("CREATE TABLE tables");
}
