
use crate::machine::Table;
use crate::machine::Machine;

use crate::storage::Tuple;
use crate::storage::format_table_name;
use crate::storage::update_tuples as update_tuples_storage;
use crate::storage::flush_page;

pub fn update_tuples(machine: &mut Machine, table: &Table, tuples: &mut Vec<Tuple>) {
    let page_key = format_table_name(&table.database_name, &table.name);

    update_tuples_storage(&mut machine.pager, &page_key, tuples);
    flush_page(&mut machine.pager, &page_key);
}
