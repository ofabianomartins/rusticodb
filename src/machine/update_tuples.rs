
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::pager_manager_flush_page;
use crate::machine::pager_manager_update_tuples;

use crate::storage::Tuple;
use crate::storage::format_table_name;

pub fn update_tuples(machine: &mut Machine, table: &Table, tuples: &mut Vec<Tuple>) {
    let page_key = format_table_name(&table.database_name, &table.name);

    pager_manager_update_tuples(&mut machine.pager_manager, &page_key, tuples);
    pager_manager_flush_page(&mut machine.pager_manager, &page_key);
}
