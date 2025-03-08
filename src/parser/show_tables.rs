use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::raw_val::RawVal;
use crate::machine::product_cartesian;
use crate::machine::Expression;
use crate::machine::Expression2Type;

use crate::utils::ExecutionError;

use crate::config::SysDb;

pub fn show_tables(machine: &mut Machine) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let result_set = product_cartesian(machine, vec![SysDb::table_tables()]);

        let condition = Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("database_name"))),
            Box::new(Expression::Const(RawVal::Str(db_name)))
        );

        return Ok(result_set.selection(condition).unwrap());
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

