use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::raw_val::RawVal;
use crate::machine::Condition;
use crate::machine::Condition2Type;
use crate::machine::drop_tuples;

use crate::machine::get_columns::get_columns_table_definition;

use crate::sys_db::SysDb;

pub fn drop_columns(machine: &mut Machine, table: &Table) {
    let condition = Condition::Func2(
        Condition2Type::And,
        Box::new(Condition::Func2(
            Condition2Type::Equal,
            Box::new(Condition::ColName(String::from("database_name"))),
            Box::new(Condition::Const(RawVal::Str(table.database_name.clone())))
        )),
        Box::new(Condition::Func2(
            Condition2Type::Equal,
            Box::new(Condition::ColName(String::from("table_name"))),
            Box::new(Condition::Const(RawVal::Str(table.name.clone())))
        ))
    );

    drop_tuples(machine, &SysDb::table_columns(), get_columns_table_definition(), &condition);
}
