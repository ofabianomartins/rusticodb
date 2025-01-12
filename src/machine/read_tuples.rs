use crate::machine::Machine;
use crate::machine::Table;

use crate::storage::Tuple;

use crate::utils::Logger;

pub fn read_tuples(machine: &mut Machine, table: &Table) -> Vec<Tuple> {
    Logger::debug(format!("Reading ({}, {})", table.database_name, table.name).leak());
    return machine.pager.read_tuples(&table.database_name, &table.name)
}

