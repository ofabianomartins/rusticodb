use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::get_columns;
use crate::machine::drop_tuples;

use crate::storage::RawVal;
use crate::storage::Expression;
use crate::storage::Expression2Type;

use crate::config::SysDb;

pub fn drop_table_ref(machine: &mut Machine, table: &Table) {
    let columns = get_columns(machine, &SysDb::table_tables());

    let condition = Expression::Func2(
        Expression2Type::And,
        Box::new(Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("database_name"))),
            Box::new(Expression::Const(RawVal::Str(table.database_name.clone())))
        )),
        Box::new(Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("name"))),
            Box::new(Expression::Const(RawVal::Str(table.name.clone())))
        ))
    );

    drop_tuples(machine, &SysDb::table_tables(), columns, &condition);
}
