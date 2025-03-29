use crate::machine::Machine;
use crate::machine::get_columns;
use crate::machine::read_tuples;

use crate::storage::Data;
use crate::storage::Expression;
use crate::storage::Expression2Type;
use crate::storage::Tuple;

use crate::config::SysDb;

pub fn check_database_exists(machine: &mut Machine, database_name: &String) -> bool {
    let condition = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("name"))),
        Box::new(Expression::Const(Data::Varchar(database_name.clone())))
    );

    let columns = get_columns(machine, &SysDb::table_databases()).iter().map(|e| e.name.clone()).collect();
    let tuples: Vec<Tuple> = read_tuples(machine, &SysDb::table_databases())
        .into_iter()
        .filter(|tuple| condition.result(tuple, &columns).is_true())
        .collect();

    return tuples.len() > 0;
}
