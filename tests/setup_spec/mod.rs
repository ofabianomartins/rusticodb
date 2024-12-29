use rusticodb::config::Config;
use rusticodb::machine::context::Context;
use rusticodb::machine::machine::Machine;
use rusticodb::machine::table::Table;
use rusticodb::storage::tuple::Tuple;
use rusticodb::storage::pager::Pager;
use rusticodb::setup::setup_system;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_system_database_setup_with_database_to_load() {
    let pager = Pager::new();
    let context = Context::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = machine.create_database(Config::system_database().to_string(), false);
    let _ = machine.create_table(&Config::system_database(), &Config::system_database_table_databases(), false, Vec::new());

    let mut tuples: Vec<Tuple> = Vec::new();

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_string(&Config::system_database());
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(2u64);
    tuple.push_string(&String::from("database1"));
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(3u64);
    tuple.push_string(&String::from("database2"));
    tuples.push(tuple);

    let table_databases = Table::new(Config::system_database(), Config::system_database_table_databases());
    machine.insert_tuples(&table_databases, &mut tuples);

    setup_system(&mut machine);

    assert!(machine.context.check_database_exists(&Config::system_database()));
    assert!(machine.context.check_database_exists(&String::from("database1")));
    assert!(machine.context.check_database_exists(&String::from("database2")));
}

#[test]
pub fn test_system_database_setup_with_tables_to_load() {
    let pager = Pager::new();
    let context = Context::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = machine.create_database(Config::system_database().to_string(), false);
    let _ = machine.create_table(&Config::system_database(), &Config::system_database_table_databases(), false, Vec::new());
    let _ = machine.create_table(&Config::system_database(), &Config::system_database_table_tables(), false, Vec::new());

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_string(&Config::system_database());
    tuples.push(tuple);
    let table_databases = Table::new(Config::system_database(), Config::system_database_table_databases());
    machine.insert_tuples(&table_databases, &mut tuples);

    let mut tuples: Vec<Tuple> = Vec::new();

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_databases());
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(2u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_tables());
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(3u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuples.push(tuple);

    let table_tables = Table::new(Config::system_database(), Config::system_database_table_tables());
    machine.insert_tuples(&table_tables, &mut tuples);

    setup_system(&mut machine);

    let table1 = Table::new(Config::system_database(), Config::system_database_table_databases());
    let table2 = Table::new(Config::system_database(), Config::system_database_table_tables());

    assert!(machine.context.check_database_exists(&Config::system_database()));
    assert!(machine.context.check_table_exists(&table1));
    assert!(machine.context.check_table_exists(&table2));
}

#[test]
pub fn test_system_database_setup_with_columns_to_load() {
    let pager = Pager::new();
    let context = Context::new();
    let mut machine = Machine::new(pager, context);

    create_tmp_test_folder();

    let _ = machine.create_database(Config::system_database().to_string(), false);
    let _ = machine.create_table(&Config::system_database(), &Config::system_database_table_databases(), false, Vec::new());

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_string(&Config::system_database());
    tuples.push(tuple);
    let table_databases = Table::new(Config::system_database(), Config::system_database_table_databases());
    machine.insert_tuples(&table_databases, &mut tuples);

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_databases());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(2u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_tables());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuples.push(tuple);

    let table_columns = Table::new(Config::system_database(), Config::system_database_table_columns());
    machine.insert_tuples(&table_columns, &mut tuples);

    setup_system(&mut machine);

    assert!(machine.context.check_database_exists(&Config::system_database()));
    assert!(
        machine.context.check_column_exists(
            &Config::system_database(),
            &Config::system_database_table_databases(),
            &String::from("name")
        )
    );
    assert!(
        machine.context.check_column_exists(
            &Config::system_database(), 
            &Config::system_database_table_tables(), 
            &String::from("name")
        )
    );
}
