use crate::machine::Column;
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::pager_manager_read_tuples;
use crate::machine::pager_manager_update_tuples;
use crate::machine::pager_manager_flush_page;

use crate::storage::Expression;
use crate::storage::Tuple;
use crate::storage::format_table_name;

pub fn drop_tuples(machine: &mut Machine, table: &Table, columns: Vec<Column>, condition: &Expression) {
    let page_key = format_table_name(&table.database_name, &table.name);

    let mut tuples: Vec<Tuple> = pager_manager_read_tuples(&mut machine.pager_manager, &page_key)
        .into_iter()
        .filter(|tuple| !&condition.result(tuple, &columns.iter().map(|e| e.name.clone() ).collect()).is_true())
        .collect();

    pager_manager_update_tuples(&mut machine.pager_manager, &page_key, &mut tuples);
    pager_manager_flush_page(&mut machine.pager_manager, &page_key);
}
