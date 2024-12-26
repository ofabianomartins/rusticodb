use crate::config::Config;
use crate::machine::column::ColumnType;
use crate::machine::machine::Machine;
use crate::storage::tuple::Tuple;
use crate::storage::os_interface::OsInterface;
use crate::utils::logger::Logger;

pub fn setup_columns_table(machine: &mut Machine) {
    Logger::info("setup columns table");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_databases());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_tables());
    tuple.push_string(&String::from("database_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_tables());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuple.push_string(&String::from("database_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuple.push_string(&String::from("table_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuple.push_string(&String::from("type"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuple.push_string(&String::from("not_null"));
    tuple.push_string(&String::from("TINYINT"));
    tuple.push_boolean(true);
    tuples.push(tuple);

    OsInterface::create_file(
        &machine.pager.format_table_name(
            &Config::system_database(),
            &Config::system_database_table_columns()
        )
    );
    machine.insert_tuples(
        &Config::system_database(),
        &Config::system_database_table_columns(),
        &mut tuples
    );
}

pub fn load_columns_table(machine: &mut Machine) {
    Logger::info("loading columns table");

    let mut tuples: Vec<Tuple> = machine.read_tuples(
        &Config::system_database(), 
        &Config::system_database_table_columns()
    );

    for tuple in tuples.iter_mut() {
        machine.context.add_column(
            tuple.get_string(0).unwrap(), 
            tuple.get_string(1).unwrap(),
            tuple.get_string(2).unwrap(),
            ColumnType::Varchar
        );
    }
}
