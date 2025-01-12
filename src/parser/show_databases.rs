use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::ResultSet;
use crate::machine::product_cartesian;

use crate::utils::ExecutionError;

pub fn show_databases(machine: &mut Machine) -> Result<ResultSet, ExecutionError> { 
    let table = Table::new(String::from("rusticodb"), String::from("tables"));
    
    return Ok(product_cartesian(machine, vec![table]));
}

