use crate::config::Config;

use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::raw_val::RawVal;
use crate::machine::Condition;
use crate::machine::Condition2Type;
use crate::machine::get_columns;
use crate::machine::read_tuples;


use crate::storage::Tuple;

pub fn check_database_exists(machine: &mut Machine, database_name: &String) -> bool {
    let table_databases = Table::new(
        Config::sysdb(),
        Config::sysdb_table_databases()
    );

    let condition = Condition::Func2(
        Condition2Type::Equal,
        Box::new(Condition::ColName(String::from("name"))),
        Box::new(Condition::Const(RawVal::Str(database_name.clone())))
    );

    let columns = get_columns(machine, &table_databases);
    let tuples: Vec<Tuple> = read_tuples(machine, &table_databases)
        .into_iter()
        .filter(|tuple| condition.evaluate(tuple, &columns))
        .collect();

    return tuples.len() > 0;
}
