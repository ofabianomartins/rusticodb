use crate::config::Config;
use crate::storage::machine::Machine;
use crate::storage::os_interface::OsInterface;

pub fn setup_system(machine: &mut Machine) {
    OsInterface::create_folder_if_not_exists(&Config::data_folder());

    let main_db_name = String::from("rusticodb");

    machine.create_database(&main_db_name);
    machine.create_table(&main_db_name, &String::from("databases"));
    machine.create_table(&main_db_name, &String::from("tables"));
    machine.create_table(&main_db_name, &String::from("columns"));
}
