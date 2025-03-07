mod load_databases;
mod load_tables;
mod load_sequences;
mod load_indexes;

use crate::config::Config;

use crate::setup::load_databases::setup_databases_table;
use crate::setup::load_tables::setup_tables_table;
use crate::setup::load_sequences::setup_sequences_table;
use crate::setup::load_indexes::setup_indexes_table;

use crate::machine::Machine;
use crate::machine::path_exists;
use crate::machine::database_exists;

use crate::storage::os_interface::create_folder_if_not_exists;
use crate::storage::format_database_name;

use crate::sys_db::SysDb;

use crate::utils::Logger;

pub fn setup_system(machine: &mut Machine) {
    create_folder_if_not_exists(&Config::data_folder());

    Logger::info("Initializing setup!");
    load_context(machine);
    Logger::info("Finalizing setup!");
}

pub fn load_context(machine: &mut Machine) {
    if database_exists(machine, &Config::sysdb()) == false {
        Logger::warn("rusticodb does not exists");
        create_folder_if_not_exists(&format_database_name(&Config::sysdb()));
    }

    if path_exists(machine,&SysDb::table_sequences()) == false {
        setup_sequences_table(machine);
    }

    if path_exists(machine,&SysDb::table_indexes()) == false {
        setup_indexes_table(machine);
    }

    if path_exists(machine,&SysDb::table_databases()) == false {
        setup_databases_table(machine);
    }

    if path_exists(machine,&SysDb::table_tables()) == false {
        setup_tables_table(machine);
    }
}
