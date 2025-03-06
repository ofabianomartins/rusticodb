use crate::machine::Machine;

use crate::storage::os_interface::path_exists;
use crate::storage::format_database_name;

pub fn database_exists(_machine: &mut Machine, database_name: &String) -> bool {
    return path_exists(&format_database_name(database_name));
}
