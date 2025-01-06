use crate::config::Config;
use crate::machine::Machine;
use crate::machine::table::Table;
use crate::storage::tuple::Tuple;
use crate::storage::os_interface::OsInterface;
use crate::utils::logger::Logger;

pub fn setup_columns_table(machine: &mut Machine) {
    Logger::info("setup columns table");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_databases());
    tuple.push_string(&String::from("id"));
    tuple.push_string(&String::from("BIGINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(3u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_databases());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(3u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_tables());
    tuple.push_string(&String::from("id"));
    tuple.push_string(&String::from("BIGINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(4u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_tables());
    tuple.push_string(&String::from("database_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(5u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_tables());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(6u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuple.push_string(&String::from("id"));
    tuple.push_string(&String::from("BIGINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(7u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuple.push_string(&String::from("database_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(8u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuple.push_string(&String::from("table_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(9u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(10u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuple.push_string(&String::from("type"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(11u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuple.push_string(&String::from("not_null"));
    tuple.push_string(&String::from("TINYINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(12u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuple.push_string(&String::from("unique"));
    tuple.push_string(&String::from("TINYINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(13u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_columns());
    tuple.push_string(&String::from("primary_key"));
    tuple.push_string(&String::from("TINYINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(14u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_sequences());
    tuple.push_string(&String::from("id"));
    tuple.push_string(&String::from("BIGINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(15u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_sequences());
    tuple.push_string(&String::from("database_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(16u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_sequences());
    tuple.push_string(&String::from("table_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(17u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_sequences());
    tuple.push_string(&String::from("column_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(18u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_sequences());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(19u64);
    tuple.push_string(&Config::system_database());
    tuple.push_string(&Config::system_database_table_sequences());
    tuple.push_string(&String::from("next_id"));
    tuple.push_string(&String::from("BITGINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let table = Table::new(
        Config::system_database(),
        Config::system_database_table_columns()
    );

    OsInterface::create_file(&machine.pager.format_table_name(&table.database_name, &table.name));
    machine.insert_tuples(&table, &mut tuples);
}
