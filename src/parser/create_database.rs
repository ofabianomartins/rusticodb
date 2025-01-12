use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::create_database as machine_create_database;

use crate::utils::ExecutionError;

pub fn create_database(machine: &mut Machine, db_name: String, if_not_exists: bool) -> Result<ResultSet, ExecutionError> { 
    machine_create_database(machine, db_name.to_string(), if_not_exists)
}

