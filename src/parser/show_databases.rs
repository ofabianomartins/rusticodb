use crate::machine::Machine;
use crate::machine::product_cartesian;

use crate::storage::ResultSet;

use crate::utils::ExecutionError;

use crate::config::SysDb;

pub fn show_databases(machine: &mut Machine) -> Result<ResultSet, ExecutionError> { 
    return Ok(product_cartesian(machine, vec![SysDb::table_databases()]));
}

