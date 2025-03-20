use crate::machine::Table;
use crate::machine::Machine;

use crate::storage::create_file as create_file_storage;
use crate::storage::format_table_name;

pub fn create_file(_machine: &mut Machine, table: &Table) {
    create_file_storage(&format_table_name(&table.database_name, &table.name));
}
