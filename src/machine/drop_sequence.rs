use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::drop_tuples;
use crate::machine::get_columns;

use crate::storage::RawVal;
use crate::storage::Expression;
use crate::storage::Expression2Type;

use crate::utils::ExecutionError;

use crate::config::SysDb;

pub fn drop_sequence(machine: &mut Machine, index_name: &String) -> Result<ResultSet, ExecutionError>{
    let columns = get_columns(machine, &SysDb::table_sequences());

    let condition = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("name"))),
        Box::new(Expression::Const(RawVal::Str(index_name.clone())))
    );

    drop_tuples(machine, &SysDb::table_sequences(), columns, &condition);

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP SEQUENCE")))
}
