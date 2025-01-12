use crate::machine::Machine;

use crate::storage::OsInterface;

pub fn database_exists(machine: &mut Machine, database_name: &String) -> bool {
    return OsInterface::path_exists(&machine.pager.format_database_name(database_name));
}
