
use crate::machine::Table;
use crate::machine::Machine;

use crate::storage::Tuple;

pub fn insert_tuples(machine: &mut Machine, table: &Table, tuples: &mut Vec<Tuple>) {
    machine.pager.insert_tuples(&table.database_name, &table.name, tuples);
    machine.pager.flush_page(&table.database_name, &table.name);
}
