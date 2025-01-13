use crate::config::Config;

use crate::machine::Machine;
use crate::machine::insert_tuples;
use crate::machine::create_file;

use crate::storage::Tuple;

use crate::utils::Logger;

use crate::sys_db::SysDb;

pub fn setup_tables_table(machine: &mut Machine) {
    Logger::info("setup tables table");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_databases());
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(2u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_tables());
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(3u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_columns());
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(4u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_sequences());
    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(5u64);
    tuple.push_string(&Config::sysdb());
    tuple.push_string(&Config::sysdb_table_indexes());
    tuples.push(tuple);

    create_file(machine, &SysDb::table_tables());
    insert_tuples(machine, &SysDb::table_tables(), &mut tuples);
}
