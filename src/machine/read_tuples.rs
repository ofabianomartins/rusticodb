use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::pager_manager_read_tuples;

use crate::storage::Tuple;
use crate::storage::format_table_name;

use crate::utils::Logger;

pub fn read_tuples(machine: &mut Machine, table: &Table) -> Vec<Tuple> {
    let page_key = format_table_name(&table.database_name, &table.name);

    Logger::debug(format!("read tuples from {}",page_key).leak());
    
    return pager_manager_read_tuples(&mut machine.pager_manager, &page_key)
}

