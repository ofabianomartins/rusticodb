use crate::config::Config;

use crate::machine::Machine;
use crate::machine::insert_tuples;
use crate::machine::create_file;

use crate::storage::Tuple;
use crate::storage::get_tuple_column;

use crate::sys_db::SysDb;

use crate::utils::Logger;


pub fn setup_columns_table(machine: &mut Machine) {
    Logger::info("setup columns table");

    let mut tuples: Vec<Tuple> = Vec::new();
    tuples.push(get_tuple_column(01u64, &Config::sysdb(), &Config::sysdb_table_databases(), &String::from("id"), &String::from("BIGINT"), true, true, true, &String::from("")));
    tuples.push(get_tuple_column(02u64, &Config::sysdb(), &Config::sysdb_table_databases(), &String::from("name"), &String::from("VARCHAR"), true, false, false, &String::from("")));

    tuples.push(get_tuple_column(03u64, &Config::sysdb(), &Config::sysdb_table_tables(), &String::from("id"), &String::from("BIGINT"), true, true, true, &String::from("")));
    tuples.push(get_tuple_column(04u64, &Config::sysdb(), &Config::sysdb_table_tables(), &String::from("database_name"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(05u64, &Config::sysdb(), &Config::sysdb_table_tables(), &String::from("name"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(06u64, &Config::sysdb(), &Config::sysdb_table_tables(), &String::from("type"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(07u64, &Config::sysdb(), &Config::sysdb_table_tables(), &String::from("query"), &String::from("VARCHAR"), false, false, false, &String::from("")));

    tuples.push(get_tuple_column(08u64, &Config::sysdb(), &Config::sysdb_table_columns(), &String::from("id"), &String::from("BIGINT"), true, true, true, &String::from("")));
    tuples.push(get_tuple_column(09u64, &Config::sysdb(), &Config::sysdb_table_columns(), &String::from("database_name"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(10u64, &Config::sysdb(), &Config::sysdb_table_columns(), &String::from("table_name"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(11u64, &Config::sysdb(), &Config::sysdb_table_columns(), &String::from("name"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(12u64, &Config::sysdb(), &Config::sysdb_table_columns(), &String::from("type"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(13u64, &Config::sysdb(), &Config::sysdb_table_columns(), &String::from("not_null"), &String::from("TINYINT"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(14u64, &Config::sysdb(), &Config::sysdb_table_columns(), &String::from("unique"), &String::from("TINYINT"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(15u64, &Config::sysdb(), &Config::sysdb_table_columns(), &String::from("primary_key"), &String::from("TINYINT"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(16u64, &Config::sysdb(), &Config::sysdb_table_columns(), &String::from("default"), &String::from("VARCHAR"), true, false, false, &String::from("")));

    tuples.push(get_tuple_column(17u64, &Config::sysdb(), &Config::sysdb_table_sequences(), &String::from("id"), &String::from("BIGINT"), true, true, true, &String::from("")));
    tuples.push(get_tuple_column(18u64, &Config::sysdb(), &Config::sysdb_table_sequences(), &String::from("database_name"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(19u64, &Config::sysdb(), &Config::sysdb_table_sequences(), &String::from("table_name"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(20u64, &Config::sysdb(), &Config::sysdb_table_sequences(), &String::from("column_name"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(21u64, &Config::sysdb(), &Config::sysdb_table_sequences(), &String::from("name"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(22u64, &Config::sysdb(), &Config::sysdb_table_sequences(), &String::from("next_id"), &String::from("BIGINT"), true, false, false, &String::from("")));

    tuples.push(get_tuple_column(23u64, &Config::sysdb(), &Config::sysdb_table_indexes(), &String::from("id"), &String::from("BIGINT"), true, true, true, &String::from("")));
    tuples.push(get_tuple_column(24u64, &Config::sysdb(), &Config::sysdb_table_indexes(), &String::from("database_name"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(25u64, &Config::sysdb(), &Config::sysdb_table_indexes(), &String::from("table_name"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(26u64, &Config::sysdb(), &Config::sysdb_table_indexes(), &String::from("column_name"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(27u64, &Config::sysdb(), &Config::sysdb_table_indexes(), &String::from("name"), &String::from("VARCHAR"), true, false, false, &String::from("")));
    tuples.push(get_tuple_column(28u64, &Config::sysdb(), &Config::sysdb_table_indexes(), &String::from("type"), &String::from("VARCHAR"), true, false, false, &String::from("")));

    create_file(machine, &SysDb::table_columns());
    insert_tuples(machine, &SysDb::table_columns(), &mut tuples);
}
