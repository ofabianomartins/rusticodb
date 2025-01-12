mod load_databases;
mod load_tables;
mod load_columns;
mod load_sequences;

use crate::config::Config;

use crate::setup::load_databases::setup_databases_table;
use crate::setup::load_tables::setup_tables_table;
use crate::setup::load_columns::setup_columns_table;
use crate::setup::load_sequences::setup_sequences_table;

use crate::machine::Machine;
use crate::machine::create_database;
use crate::machine::path_exists;
use crate::machine::check_database_exists;

use crate::storage::OsInterface;

use crate::sys_db::SysDb;

use crate::utils::Logger;

pub fn setup_system(machine: &mut Machine) {
    OsInterface::create_folder_if_not_exists(&Config::data_folder());

    Logger::info("Initializing setup!");
    load_context(machine);
    Logger::info("Finalizing setup!");
}

pub fn load_context(machine: &mut Machine) {
    if check_database_exists(machine, &Config::sysdb()) == false {
        Logger::warn("rusticodb does not exists");
        let _ = create_database(machine, Config::sysdb(), true);
    }

    if path_exists(machine,&SysDb::table_databases()) == false {
        setup_databases_table(machine);
    }

    if path_exists(machine,&SysDb::table_tables()) == false {
        setup_tables_table(machine);
    }

    if path_exists(machine,&SysDb::table_columns()) == false {
        setup_columns_table(machine);
    }

    if path_exists(machine,&SysDb::table_sequences()) == false {
        setup_sequences_table(machine);
    }
}
