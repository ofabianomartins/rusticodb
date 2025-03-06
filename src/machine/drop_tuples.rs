use crate::machine::Column;
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::Expression;

use crate::storage::Tuple;
use crate::storage::format_table_name;
use crate::storage::read_tuples;
use crate::storage::update_tuples;
use crate::storage::flush_page;

pub fn drop_tuples(machine: &mut Machine, table: &Table, columns: Vec<Column>, condition: &Expression) {
    let page_key = format_table_name(&table.database_name, &table.name);

    let mut tuples: Vec<Tuple> = read_tuples(&mut machine.pager, &page_key)
        .into_iter()
        .filter(|tuple| !condition.result(tuple, &columns).is_true())
        .collect();

    update_tuples(&mut machine.pager, &page_key, &mut tuples);
    flush_page(&mut machine.pager, &page_key);
}
