use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::product_cartesian;

use crate::utils::ExecutionError;
use crate::sys_db::SysDb;

pub fn show_databases(machine: &mut Machine) -> Result<ResultSet, ExecutionError> { 
    return Ok(product_cartesian(machine, vec![SysDb::table_tables()]));
}

