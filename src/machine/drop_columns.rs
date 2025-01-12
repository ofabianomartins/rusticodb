use crate::config::Config;

use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::raw_val::RawVal;
use crate::machine::Condition;
use crate::machine::Condition2Type;
use crate::machine::drop_tuples;

use crate::machine::get_columns::get_columns_table_definition;


pub fn drop_columns(machine: &mut Machine, table: &Table) {
    let table_columns = Table::new(
        Config::sysdb(),
        Config::sysdb_table_columns()
    );

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

    drop_tuples(machine, &table_columns, get_columns_table_definition(), &condition);
}
