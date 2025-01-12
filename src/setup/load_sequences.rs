
use crate::machine::Machine;
use crate::machine::insert_tuples;
use crate::machine::create_file;

use crate::sys_db::SysDb;
use crate::utils::Logger;

pub fn setup_sequences_table(machine: &mut Machine) {
    Logger::info("setup tables table");

    create_file(machine, &SysDb::table_sequences());
    insert_tuples(machine, &SysDb::table_sequences(), &mut Vec::new());
}
