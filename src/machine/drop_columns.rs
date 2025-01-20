use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::raw_val::RawVal;
use crate::machine::get_columns_table_definition;
use crate::machine::drop_tuples;
use crate::machine::Expression;
use crate::machine::Expression2Type;

use crate::sys_db::SysDb;

pub fn drop_columns(machine: &mut Machine, table: &Table) {
    let condition = Expression::Func2(
        Expression2Type::And,
        Box::new(Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("database_name"))),
            Box::new(Expression::Const(RawVal::Str(table.database_name.clone())))
        )),
        Box::new(Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("table_name"))),
            Box::new(Expression::Const(RawVal::Str(table.name.clone())))
        ))
    );

    drop_tuples(machine, &SysDb::table_columns(), get_columns_table_definition(), &condition);
}
