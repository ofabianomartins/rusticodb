
use crate::config::Config;

use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::insert_tuples;

use crate::storage::tuple::Tuple;
use crate::storage::os_interface::OsInterface;

use crate::utils::logger::Logger;


pub fn setup_sequences_table(machine: &mut Machine) {
    Logger::info("setup tables table");

    let mut tuples: Vec<Tuple> = Vec::new();

    OsInterface::create_file(
        &machine.pager.format_table_name(
            &Config::system_database(),
            &Config::system_database_table_sequences()
        )
    );

    let table = Table::new(
        Config::system_database(),
        Config::system_database_table_sequences()
    );
    insert_tuples(machine, &table, &mut tuples);
}
