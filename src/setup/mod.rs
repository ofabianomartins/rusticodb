mod load_databases;
mod load_tables;
mod load_columns;

use crate::config::Config;
use crate::machine::machine::Machine;
use crate::storage::os_interface::OsInterface;
use crate::utils::logger::Logger;

use crate::setup::load_databases::setup_databases_table;
use crate::setup::load_databases::load_databases_table;

use crate::setup::load_tables::setup_tables_table;
use crate::setup::load_tables::load_tables_table;

use crate::setup::load_columns::setup_columns_table;
use crate::setup::load_columns::load_columns_table;

pub fn setup_system(machine: &mut Machine) {
    OsInterface::create_folder_if_not_exists(&Config::data_folder());

    Logger::info("Initializing setup!");
    load_context(machine);
    Logger::info("Finalizing setup!");
}

pub fn load_context(machine: &mut Machine) {
    if machine.database_exists(&Config::system_database()) == false {
        Logger::warn("rusticodb does not exists");
        let _ = machine.create_database(Config::system_database(), true);
    }

    if OsInterface::path_exists(
        &machine.pager.format_table_name(
            &Config::system_database(),
            &Config::system_database_table_databases()
        )
    ) == false{
        setup_databases_table(machine);
    }

    if OsInterface::path_exists(
        &machine.pager.format_table_name(
            &Config::system_database(),
            &Config::system_database_table_tables()
        )
    ) == false{
        setup_tables_table(machine);
    }

    if OsInterface::path_exists(
        &machine.pager.format_table_name(
            &Config::system_database(),
            &Config::system_database_table_columns()
        )
    ) == false{
        setup_columns_table(machine);
    }

    load_databases_table(machine);
    load_tables_table(machine);
    load_columns_table(machine);
}
