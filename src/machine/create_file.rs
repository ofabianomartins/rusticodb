use crate::machine::Table;
use crate::machine::Machine;

use crate::storage::OsInterface;
use crate::storage::format_table_name;

pub fn create_file(_machine: &mut Machine, table: &Table) {
    OsInterface::create_file(
        &format_table_name(&table.database_name, &table.name)
    );
}
