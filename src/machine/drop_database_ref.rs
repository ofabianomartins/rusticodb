use crate::config::Config;

use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::raw_val::RawVal;
use crate::machine::Condition;
use crate::machine::Condition2Type;
use crate::machine::get_columns;
use crate::machine::drop_tuples;

pub fn drop_database_ref(machine: &mut Machine, database_name: &String) {
    let table_databases = Table::new(
        Config::system_database(),
        Config::system_database_table_databases()
    );

    let columns = get_columns(machine, &table_databases);

    let condition = Condition::Func2(
        Condition2Type::Equal,
        Box::new(Condition::ColName(String::from("name"))),
        Box::new(Condition::Const(RawVal::Str(database_name.clone())))
    );

    drop_tuples(machine, &table_databases, columns, &condition);
}
