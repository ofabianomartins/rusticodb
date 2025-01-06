use crate::config::Config;
use crate::machine::Machine;
use crate::machine::table::Table;
use crate::storage::tuple::Tuple;
use crate::storage::os_interface::OsInterface;
use crate::utils::logger::Logger;

pub fn setup_databases_table(machine: &mut Machine) {
    Logger::info("setup databases table");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_string(&Config::system_database());
    tuples.push(tuple);

    let table = Table::new(
        Config::system_database(),
        Config::system_database_table_databases()
    );

    OsInterface::create_file(
        &machine.pager.format_table_name(
            &table.database_name,
            &table.name
        )
    );
    machine.insert_tuples(&table, &mut tuples);
}
