use crate::config::Config;

use crate::machine::Machine;
use crate::machine::insert_tuples;
use crate::machine::create_file;

use crate::storage::Tuple;

use crate::utils::Logger;

use crate::sys_db::SysDb;

pub fn get_tuple_table(id: u64, db_name: &String, name: &String) -> Tuple {
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(id);
    tuple.push_string(db_name);
    tuple.push_string(name);
    return tuple;
}

pub fn setup_tables_table(machine: &mut Machine) {
    Logger::info("setup tables table");

    let mut tuples: Vec<Tuple> = Vec::new();

    tuples.push(get_tuple_table(1u64, &Config::sysdb(), &Config::sysdb_table_databases()));
    tuples.push(get_tuple_table(2u64, &Config::sysdb(), &Config::sysdb_table_tables()));
    tuples.push(get_tuple_table(3u64, &Config::sysdb(), &Config::sysdb_table_columns()));
    tuples.push(get_tuple_table(4u64, &Config::sysdb(), &Config::sysdb_table_sequences()));
    tuples.push(get_tuple_table(5u64, &Config::sysdb(), &Config::sysdb_table_indexes()));

    create_file(machine, &SysDb::table_tables());
    insert_tuples(machine, &SysDb::table_tables(), &mut tuples);
}
