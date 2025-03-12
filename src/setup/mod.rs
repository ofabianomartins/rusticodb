mod load_base;

use crate::config::Config;
use crate::config::SysDb;

use crate::setup::load_base::setup_base;

use crate::machine::Machine;
use crate::machine::path_exists;
use crate::machine::database_exists;

use crate::storage::os_interface::create_folder_if_not_exists;
use crate::storage::format_database_name;

use crate::utils::Logger;

pub fn setup_system(machine: &mut Machine) {
    create_folder_if_not_exists(&Config::data_folder());

    Logger::info("Initializing setup!");
    load_context(machine);
    Logger::info("Finalizing setup!");
}

pub fn load_context(machine: &mut Machine) {
    if database_exists(machine, &SysDb::dbname()) == false {
        Logger::warn("rusticodb does not exists");
        create_folder_if_not_exists(&format_database_name(&SysDb::dbname()));
    }

    if path_exists(machine,&SysDb::table_sequences()) == false {
        setup_base(machine);
    }

}
