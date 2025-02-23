use crate::config::Config;

use crate::machine::Machine;
use crate::machine::insert_tuples;
use crate::machine::create_file;

use crate::storage::Tuple;

use crate::sys_db::SysDb;
use crate::utils::Logger;

pub fn get_tuple_database(id: u64, name: &String) -> Tuple {
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(id);
    tuple.push_string(name);
    return tuple;
}

pub fn setup_databases_table(machine: &mut Machine) {
    Logger::info("setup databases table");

    let mut tuples: Vec<Tuple> = Vec::new();
    tuples.push(get_tuple_database(1u64, &Config::sysdb()));

    create_file(machine, &SysDb::table_databases());
    insert_tuples(machine, &SysDb::table_databases(), &mut tuples);
}
