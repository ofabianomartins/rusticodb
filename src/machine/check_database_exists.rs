use crate::config::Config;

use crate::machine::Machine;
use crate::machine::Table;
use crate::machine::raw_val::RawVal;
use crate::machine::get_columns;
use crate::machine::read_tuples;
use crate::machine::Expression;
use crate::machine::Expression2Type;

use crate::storage::Tuple;

pub fn check_database_exists(machine: &mut Machine, database_name: &String) -> bool {
    let table_databases = Table::new(
        Config::sysdb(),
        Config::sysdb_table_databases()
    );

    let condition = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("name"))),
        Box::new(Expression::Const(RawVal::Str(database_name.clone())))
    );

    let columns = get_columns(machine, &table_databases);
    let tuples: Vec<Tuple> = read_tuples(machine, &table_databases)
        .into_iter()
        .filter(|tuple| condition.result(tuple, &columns).is_true())
        .collect();

    return tuples.len() > 0;
}
