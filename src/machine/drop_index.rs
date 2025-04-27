use crate::machine::Machine;
use crate::machine::drop_tuples;
use crate::machine::get_columns;

use crate::storage::Data;
use crate::storage::Expression;
use crate::storage::Expression2Type;
use crate::storage::ResultSet;
use crate::storage::ResultSetType;

use crate::utils::ExecutionError;

use crate::config::SysDb;

pub fn drop_index(machine: &mut Machine, index_name: &String) -> Result<ResultSet, ExecutionError>{
    let columns = get_columns(machine, &SysDb::table_indexes());

    let condition = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("name"))),
        Box::new(Expression::Const(Data::Varchar(index_name.clone())))
    );

    drop_tuples(machine, &SysDb::table_indexes(), columns, &condition);

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP INDEX")))
}
