use crate::config::Config;
use crate::machine::machine::Machine;
use crate::machine::context::Context;
use crate::storage::tuple::Tuple;
use crate::storage::os_interface::OsInterface;

pub fn setup_system(context: &mut Context, machine: &mut Machine) {
    OsInterface::create_folder_if_not_exists(&Config::data_folder());

    setup_context(context);
    if machine.database_exists(&Config::main_database()) == false {
        machine.create_database(&Config::main_database());
    }
    setup_databases_table(machine);
    setup_tables_table(machine);
    setup_columns_table(machine);
}

pub fn setup_context(context: &mut Context) {
    context.add_database(Config::main_database());

}

pub fn setup_databases_table(machine: &mut Machine) {
    let main_db_name = String::from("rusticodb");
    let table_name_databases = String::from("databases");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&String::from("rusticodb"));
    tuples.push(tuple);

    machine.create_table(&main_db_name, &table_name_databases);
    machine.insert_tuples(&main_db_name, &table_name_databases, &mut tuples);
}

pub fn setup_tables_table(machine: &mut Machine) {
    let main_db_name = String::from("rusticodb");
    let table_name_tables = String::from("tables");

    let mut tuples: Vec<Tuple> = Vec::new();

    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&String::from("rusticodb"));
    tuple.push_string(&String::from("databases"));
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&String::from("rusticodb"));
    tuple.push_string(&String::from("tables"));
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&String::from("rusticodb"));
    tuple.push_string(&String::from("colums"));
    tuples.push(tuple);

    machine.create_table(&main_db_name, &table_name_tables);
    machine.insert_tuples(&main_db_name, &table_name_tables, &mut tuples);
}

pub fn setup_columns_table(machine: &mut Machine) {
    let main_db_name = String::from("rusticodb");
    let table_name_tables = String::from("columns");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&String::from("rusticodb"));
    tuple.push_string(&String::from("databases"));
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuples.push(tuple);

    machine.create_table(&main_db_name, &table_name_tables);
    machine.insert_tuples(&main_db_name, &table_name_tables, &mut tuples);
}
