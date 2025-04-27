use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::get_columns_table_definition;
use crate::machine::drop_tuples;

use crate::storage::Data;
use crate::storage::Expression;
use crate::storage::Expression2Type;

use crate::config::SysDb;

pub fn drop_columns(machine: &mut Machine, table: &Table) {
    let condition = Expression::Func2(
        Expression2Type::And,
        Box::new(Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("database_name"))),
            Box::new(Expression::Const(Data::Varchar(table.database_name.clone())))
        )),
        Box::new(Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("table_name"))),
            Box::new(Expression::Const(Data::Varchar(table.name.clone())))
        ))
    );

    drop_tuples(machine, &SysDb::table_columns(), get_columns_table_definition(), &condition);
}
