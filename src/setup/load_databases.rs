use crate::config::Config;

use crate::machine::Machine;
use crate::machine::insert_tuples;
use crate::machine::create_file;

use crate::storage::Tuple;

use crate::sys_db::SysDb;
use crate::utils::Logger;

pub fn setup_databases_table(machine: &mut Machine) {
    Logger::info("setup databases table");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    tuple.push_unsigned_bigint(1u64);
    tuple.push_string(&Config::sysdb());
    tuples.push(tuple);

    create_file(machine, &SysDb::table_databases());
    insert_tuples(machine, &SysDb::table_databases(), &mut tuples);
}
