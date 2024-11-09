use crate::config::Config;
use crate::machine::machine::Machine;
use crate::storage::tuple::Tuple;
use crate::storage::os_interface::OsInterface;
use crate::utils::logger::Logger;

pub fn setup_databases_table(machine: &mut Machine) {
    Logger::info("setup databases table");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuples.push(tuple);


    OsInterface::create_file(
        &machine.pager.format_table_name(
            &Config::system_database(),
            &Config::system_database_table_databases()
        )
    );
    machine.insert_tuples(
        &Config::system_database(),
        &Config::system_database_table_databases(),
        &mut tuples
    );
}

pub fn load_databases_table(machine: &mut Machine) {
    Logger::info("loading databases table");

    let mut tuples: Vec<Tuple> = machine.read_tuples(
        &Config::system_database(), 
        &Config::system_database_table_databases()
    );

    for tuple in tuples.iter_mut() {
        machine.context.add_database(tuple.get_string(0).unwrap());
    }
}
