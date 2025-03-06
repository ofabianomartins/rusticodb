use crate::machine::Machine;

use crate::storage::OsInterface;
use crate::storage::format_database_name;

pub fn database_exists(_machine: &mut Machine, database_name: &String) -> bool {
    return OsInterface::path_exists(&format_database_name(database_name));
}
