use crate::config::Config;
use crate::machine::machine::Machine;
use crate::storage::tuple::Tuple;
use crate::storage::os_interface::OsInterface;

pub fn setup_system(machine: &mut Machine) {
    OsInterface::create_folder_if_not_exists(&Config::data_folder());

    load_context(machine);
}

pub fn load_context(machine: &mut Machine) {
    if machine.database_exists(&Config::system_database()) == false {
        let _ = machine.create_database(Config::system_database(), true);
    }

    if machine.table_exists(&Config::system_database(), &Config::system_database_table_databases()) == false {
        setup_databases_table(machine);
    } else {
        load_databases_table(machine);
    }

    if machine.table_exists(&Config::system_database(), &Config::system_database_table_tables()) == false {
        setup_tables_table(machine);
    } else {
        load_tables_table(machine)
    }

    if machine.table_exists(&Config::system_database(), &Config::system_database_table_columns()) == false {
        setup_columns_table(machine);
    } else {
        load_columns_table(machine)
    }
}

pub fn setup_databases_table(machine: &mut Machine) {
    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuples.push(tuple);

    let _ = machine.create_table(&Config::system_database(), &Config::system_database_table_databases());
    machine.insert_tuples(&Config::system_database(), &Config::system_database_table_databases(), &mut tuples);

    load_databases_table(machine);
}

pub fn load_databases_table(machine: &mut Machine) {
    let mut tuples: Vec<Tuple> = machine.read_tuples(
        &Config::system_database(), 
        &Config::system_database_table_databases()
    );

    for tuple in tuples.iter_mut() {
        machine.context.add_database(tuple.get_string(0).unwrap());
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

    let _ = machine.create_table(&Config::system_database(), &Config::system_database_table_tables());
    machine.insert_tuples(&Config::system_database(), &Config::system_database_table_tables(), &mut tuples);

    load_tables_table(machine)
}

pub fn load_tables_table(machine: &mut Machine) {
    let mut tuples: Vec<Tuple> = machine.read_tuples(
        &Config::system_database(), 
        &Config::system_database_table_tables()
    );

    for tuple in tuples.iter_mut() {
        machine.context.add_table(tuple.get_string(0).unwrap(), tuple.get_string(1).unwrap());
    }
}

pub fn setup_columns_table(machine: &mut Machine) {
    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_databases());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuples.push(tuple);

    let _ = machine.create_table(&Config::system_database(), &Config::system_database_table_columns());
    machine.insert_tuples(&Config::system_database(), &Config::system_database_table_columns(), &mut tuples);

    load_columns_table(machine);
}

pub fn load_columns_table(machine: &mut Machine) {
    let mut tuples: Vec<Tuple> = machine.read_tuples(
        &Config::system_database(), 
        &Config::system_database_table_columns()
    );

    for tuple in tuples.iter_mut() {
        machine.context.add_column(
            tuple.get_string(0).unwrap(), 
            tuple.get_string(1).unwrap(),
            tuple.get_string(2).unwrap()
        );
    }
}
