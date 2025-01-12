use crate::config::Config;

use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::raw_val::RawVal;
use crate::machine::Condition;
use crate::machine::Condition2Type;
use crate::machine::get_columns;
use crate::machine::drop_tuples;

pub fn drop_table_ref(machine: &mut Machine, table: &Table) {
    let table_tables = Table::new(
        Config::sysdb(),
        Config::sysdb_table_tables()
    );

    let columns = get_columns(machine, &table_tables);

    let condition = Condition::Func2(
        Condition2Type::And,
        Box::new(Condition::Func2(
            Condition2Type::Equal,
            Box::new(Condition::ColName(String::from("database_name"))),
            Box::new(Condition::Const(RawVal::Str(table.database_name.clone())))
        )),
        Box::new(Condition::Func2(
            Condition2Type::Equal,
            Box::new(Condition::ColName(String::from("name"))),
            Box::new(Condition::Const(RawVal::Str(table.name.clone())))
        ))
    );

    drop_tuples(machine, &table_tables, columns, &condition);
}
