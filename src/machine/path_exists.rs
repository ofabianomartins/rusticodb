use crate::machine::Table;
use crate::machine::Machine;

use crate::storage::path_exists as path_exists_storage;
use crate::storage::format_table_name;

pub fn path_exists(_machine: &mut Machine, table: &Table) -> bool {
    return path_exists_storage(&format_table_name(&table.database_name, &table.name))
}
