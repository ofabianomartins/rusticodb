use crate::machine::machine::Machine;
use crate::machine::table::Table;
use crate::machine::result_set::ResultSet;
use crate::utils::execution_error::ExecutionError;
use crate::machine::raw_val::RawVal;
use crate::machine::condition::Condition;
use crate::machine::condition::Condition2Type;

pub fn show_tables(machine: &mut Machine) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let table = Table::new(
            String::from("rusticodb"),
            String::from("tables")
        );
        let result_set = machine.product_cartesian(vec![table]);

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

