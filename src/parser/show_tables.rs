use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::ResultSet;
use crate::machine::raw_val::RawVal;
use crate::machine::Condition;
use crate::machine::Condition2Type;
use crate::machine::product_cartesian;

use crate::utils::ExecutionError;

pub fn show_tables(machine: &mut Machine) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let table = Table::new(
            String::from("rusticodb"),
            String::from("tables")
        );
        let result_set = product_cartesian(machine, vec![table]);

        let condition = Condition::Func2(
            Condition2Type::Equal,
            Box::new(Condition::ColName(String::from("database_name"))),
            Box::new(Condition::Const(RawVal::Str(db_name)))
        );

        return Ok(result_set.selection(condition).unwrap());
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

