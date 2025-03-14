use crate::machine::Index;
use crate::machine::Machine;
use crate::machine::raw_val::RawVal;
use crate::machine::get_columns;
use crate::machine::read_tuples;
use crate::machine::Expression;
use crate::machine::Expression2Type;

use crate::config::SysDb;

use crate::storage::Tuple;
use crate::storage::tuple_get_varchar;

pub fn get_indexes(machine: &mut Machine, database_name: &String) -> Vec<Index> {
    let mut indexes: Vec<Index> = Vec::new();

    let condition = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("database_name"))),
        Box::new(Expression::Const(RawVal::Str(database_name.clone())))
    );

    let columns = get_columns(machine, &SysDb::table_indexes());

    let tuples: Vec<Tuple> = read_tuples(machine, &SysDb::table_indexes())
        .into_iter()
        .filter(|tuple| condition.result(tuple, &columns).is_true())
        .collect();

    for elem in tuples.into_iter() {
        indexes.push(Index::new(tuple_get_varchar(&elem, 4).unwrap()));
    }

    return indexes;
}
