use crate::machine::Table;
use crate::machine::Machine;

use crate::storage::OsInterface;

pub fn create_file(machine: &mut Machine, table: &Table) {
    OsInterface::create_file(
        &machine.pager.format_table_name(
            &table.database_name,
            &table.name
        )
    );
}
