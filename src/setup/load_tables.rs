use crate::config::Config;
use crate::machine::machine::Machine;
use crate::storage::tuple::Tuple;
use crate::storage::os_interface::OsInterface;
use crate::utils::logger::Logger;


pub fn setup_tables_table(machine: &mut Machine) {
    Logger::info("setup tables table");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_databases());
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_tables());
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuples.push(tuple);


    OsInterface::create_file(
        &machine.pager.format_table_name(
            &Config::system_database(),
            &Config::system_database_table_tables()
        )
    );
    machine.insert_tuples(
        &Config::system_database(),
        &Config::system_database_table_tables(),
        &mut tuples
    );
}

pub fn load_tables_table(machine: &mut Machine) {
    Logger::info("loading tables table");

    let mut tuples: Vec<Tuple> = machine.read_tuples(
        &Config::system_database(), 
        &Config::system_database_table_tables()
    );

    for tuple in tuples.iter_mut() {
        machine.context.add_table(
            tuple.get_string(0).unwrap(),
            tuple.get_string(1).unwrap()
        );
    }
}
