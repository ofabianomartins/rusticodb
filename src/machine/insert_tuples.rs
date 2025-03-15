
use crate::machine::Table;
use crate::machine::Machine;

use crate::storage::Tuple;
use crate::storage::format_table_name;
use crate::storage::pager_insert_tuples;
use crate::storage::pager_flush_page;

pub fn insert_tuples(machine: &mut Machine, table: &Table, tuples: &mut Vec<Tuple>) {
    let page_key = format_table_name(&table.database_name, &table.name);

    pager_insert_tuples(&mut machine.pager, &page_key, tuples);
    pager_flush_page(&mut machine.pager, &page_key);
}
