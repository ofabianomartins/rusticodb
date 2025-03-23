use crate::machine::Column;
use crate::machine::Table;
use crate::machine::Machine;

use crate::storage::Expression;
use crate::storage::Tuple;
use crate::storage::format_table_name;
use crate::storage::pager_read_tuples;
use crate::storage::pager_update_tuples;
use crate::storage::pager_flush_page;
use crate::storage::is_true;

pub fn drop_tuples(machine: &mut Machine, table: &Table, columns: Vec<Column>, condition: &Expression) {
    let page_key = format_table_name(&table.database_name, &table.name);

    let mut tuples: Vec<Tuple> = pager_read_tuples(&mut machine.pager, &page_key)
        .into_iter()
        .filter(|tuple| !is_true(&condition.result(tuple, &columns.iter().map(|e| e.name.clone() ).collect())))
        .collect();

    pager_update_tuples(&mut machine.pager, &page_key, &mut tuples);
    pager_flush_page(&mut machine.pager, &page_key);
}
