use crate::machine::Column;
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::Expression;

use crate::storage::Tuple;

pub fn drop_tuples(machine: &mut Machine, table: &Table, columns: Vec<Column>, condition: &Expression) {
    let mut tuples: Vec<Tuple> = machine.pager.read_tuples(&table.database_name, &table.name)
        .into_iter()
        .filter(|tuple| !condition.evaluate(tuple, &columns))
        .collect();

    machine.pager.update_tuples(&table.database_name, &table.name, &mut tuples);
    machine.pager.flush_page(&table.database_name, &table.name);
}
