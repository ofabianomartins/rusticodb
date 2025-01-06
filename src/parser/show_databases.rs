use crate::machine::Machine;
use crate::machine::table::Table;
use crate::machine::result_set::ResultSet;
use crate::utils::execution_error::ExecutionError;

pub fn show_databases(machine: &mut Machine) -> Result<ResultSet, ExecutionError> { 
    let table = Table::new(String::from("rusticodb"), String::from("tables"));
    
    return Ok(machine.product_cartesian(vec![table]));
}

