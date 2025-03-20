use crate::machine::Machine;
use crate::machine::RawVal;
use crate::machine::get_columns;
use crate::machine::drop_tuples;
use crate::machine::Expression;
use crate::machine::Expression2Type;

use crate::config::SysDb;

pub fn drop_database_ref(machine: &mut Machine, database_name: &String) {
    let columns = get_columns(machine, &SysDb::table_databases());

    let condition = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("name"))),
        Box::new(Expression::Const(RawVal::Str(database_name.clone())))
    );

    drop_tuples(machine, &SysDb::table_databases(), columns, &condition);
}
