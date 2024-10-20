use crate::config::Config;
use crate::machine::machine::Machine;
use crate::machine::context::Context;
use crate::storage::tuple::Tuple;
use crate::storage::os_interface::OsInterface;

pub fn setup_system(context: &mut Context, machine: &mut Machine) {
    OsInterface::create_folder_if_not_exists(&Config::data_folder());

    load_context(context, machine);
}

pub fn load_context(context: &mut Context, machine: &mut Machine) {
    // Verify is rusticodb databases exists
    context.add_database(Config::system_database());

    if machine.database_exists(&Config::system_database()) == false {
        machine.create_database(&Config::system_database());
    }

    // Verify is databases table exists
    context.add_table(Config::system_database(), Config::system_database_table_databases());

    if machine.table_exists(&Config::system_database(), &Config::system_database_table_databases()) == false {
        setup_databases_table(machine)
    } else {
        load_databases_table(context, machine);
    }

    // Verify is tables table exists
    context.add_table(Config::system_database(), Config::system_database_table_tables());

    if machine.table_exists(&Config::system_database(), &Config::system_database_table_tables()) == false {
        setup_tables_table(machine)
    } else {

    }

    // Verify is columns table exists
    context.add_table(Config::system_database(), Config::system_database_table_columns());

    if machine.table_exists(&Config::system_database(), &Config::system_database_table_columns()) == false {
        setup_columns_table(machine)
    } else {

    }
}

pub fn setup_databases_table(machine: &mut Machine) {
    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuples.push(tuple);

    machine.create_table(&Config::system_database(), &Config::system_database_table_databases());
    machine.insert_tuples(&Config::system_database(), &Config::system_database_table_databases(), &mut tuples);
}

pub fn load_databases_table(context: &mut Context, machine: &mut Machine) {
    let mut tuples: Vec<Tuple> = machine.read_tuples(
        &Config::system_database(), 
        &Config::system_database_table_databases()
    );

    for tuple in tuples.iter_mut() {
        context.add_database(tuple.get_string(0).unwrap());
    }
}

pub fn setup_tables_table(machine: &mut Machine) {
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

    machine.create_table(&Config::system_database(), &Config::system_database_table_tables());
    machine.insert_tuples(&Config::system_database(), &Config::system_database_table_tables(), &mut tuples);
}

pub fn setup_columns_table(machine: &mut Machine) {
    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_databases());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuples.push(tuple);

    machine.create_table(&Config::system_database(), &Config::system_database_table_columns());
    machine.insert_tuples(&Config::system_database(), &Config::system_database_table_columns(), &mut tuples);
}
