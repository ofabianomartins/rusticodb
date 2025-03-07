use crate::machine::Machine;
use crate::machine::Table;

use crate::storage::Tuple;
use crate::storage::format_table_name;
use crate::storage::read_tuples as read_tuples_storage;

use crate::utils::Logger;

pub fn read_tuples(machine: &mut Machine, table: &Table) -> Vec<Tuple> {
    let page_key = format_table_name(&table.database_name, &table.name);

    Logger::debug(format!("read tuples from {}",page_key).leak());
    
    return read_tuples_storage(&mut machine.pager, &page_key)
}

