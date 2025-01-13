use crate::config::Config;

use crate::machine::Machine;
use crate::machine::insert_tuples;
use crate::machine::create_file;

use crate::storage::Tuple;

use crate::sys_db::SysDb;

use crate::utils::Logger;

pub fn setup_columns_table(machine: &mut Machine) {
    Logger::info("setup columns table");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_databases());
    tuple.push_string(&String::from("id"));
    tuple.push_string(&String::from("BIGINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(3u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_databases());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(3u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_tables());
    tuple.push_string(&String::from("id"));
    tuple.push_string(&String::from("BIGINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(4u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_tables());
    tuple.push_string(&String::from("database_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(5u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_tables());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(6u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_columns());
    tuple.push_string(&String::from("id"));
    tuple.push_string(&String::from("BIGINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(7u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_columns());
    tuple.push_string(&String::from("database_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(8u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_columns());
    tuple.push_string(&String::from("table_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(9u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_columns());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(10u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_columns());
    tuple.push_string(&String::from("type"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(11u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_columns());
    tuple.push_string(&String::from("not_null"));
    tuple.push_string(&String::from("TINYINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(12u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_columns());
    tuple.push_string(&String::from("unique"));
    tuple.push_string(&String::from("TINYINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(13u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_columns());
    tuple.push_string(&String::from("primary_key"));
    tuple.push_string(&String::from("TINYINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(14u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_sequences());
    tuple.push_string(&String::from("id"));
    tuple.push_string(&String::from("BIGINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(15u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_sequences());
    tuple.push_string(&String::from("database_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(16u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_sequences());
    tuple.push_string(&String::from("table_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(17u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_sequences());
    tuple.push_string(&String::from("column_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(18u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_sequences());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(19u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_sequences());
    tuple.push_string(&String::from("next_id"));
    tuple.push_string(&String::from("BITGINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(20u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_indexes());
    tuple.push_string(&String::from("id"));
    tuple.push_string(&String::from("BIGINT"));
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuple.push_boolean(true);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(21u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_indexes());
    tuple.push_string(&String::from("database_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(22u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_indexes());
    tuple.push_string(&String::from("table_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(23u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_indexes());
    tuple.push_string(&String::from("column_name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(24u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_indexes());
    tuple.push_string(&String::from("name"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(25u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_indexes());
    tuple.push_string(&String::from("type"));
    tuple.push_string(&String::from("VARCHAR"));
    tuple.push_boolean(true);
    tuple.push_boolean(false);
    tuple.push_boolean(false);
    tuples.push(tuple);

    create_file(machine, &SysDb::table_columns());
    insert_tuples(machine, &SysDb::table_columns(), &mut tuples);
}
