
use crate::machine::Table;
use crate::machine::Machine;

use crate::storage::Tuple;
use crate::storage::format_table_name;
use crate::storage::insert_tuples as insert_tuples_storage;
use crate::storage::flush_page;

pub fn insert_tuples(machine: &mut Machine, table: &Table, tuples: &mut Vec<Tuple>) {
    let page_key = format_table_name(&table.database_name, &table.name);

    insert_tuples_storage(&mut machine.pager, &page_key, tuples);
    flush_page(&mut machine.pager, &page_key);
}
