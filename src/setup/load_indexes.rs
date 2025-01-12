
use crate::config::Config;

use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::insert_tuples;
use crate::machine::create_file;

use crate::storage::Tuple;

use crate::utils::Logger;

pub fn setup_sequences_table(machine: &mut Machine) {
    Logger::info("setup tables table");

    let mut tuples: Vec<Tuple> = Vec::new();

    create_file(machine, &SysDb::table_indexes());
    insert_tuples(machine, &SysDb::table_indexes(), &mut tuples);
}
