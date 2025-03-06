use crate::machine::Table;
use crate::machine::Machine;

use crate::storage::OsInterface;
use crate::storage::format_table_name;

pub fn path_exists(_machine: &mut Machine, table: &Table) -> bool {
    return OsInterface::path_exists(
        &format_table_name(&table.database_name, &table.name)
    )
}
