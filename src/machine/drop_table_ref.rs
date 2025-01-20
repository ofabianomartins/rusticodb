use crate::config::Config;

use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::raw_val::RawVal;
use crate::machine::get_columns;
use crate::machine::drop_tuples;
use crate::machine::Expression;
use crate::machine::Expression2Type;

pub fn drop_table_ref(machine: &mut Machine, table: &Table) {
    let table_tables = Table::new(
        Config::sysdb(),
        Config::sysdb_table_tables()
    );

    let columns = get_columns(machine, &table_tables);

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

    drop_tuples(machine, &table_tables, columns, &condition);
}
