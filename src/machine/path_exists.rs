use crate::machine::Table;
use crate::machine::Machine;

use crate::storage::OsInterface;

pub fn path_exists(machine: &mut Machine, table: &Table) -> bool {
    return OsInterface::path_exists(
        &machine.pager.format_table_name(
            &table.database_name,
            &table.name
        )
    )
}
