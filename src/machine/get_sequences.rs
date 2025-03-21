use crate::machine::Sequence;
use crate::machine::Machine;
use crate::machine::RawVal;
use crate::machine::get_columns;
use crate::machine::read_tuples;
use crate::machine::Expression;
use crate::machine::Expression2Type;

use crate::config::SysDb;

use crate::storage::Tuple;
use crate::storage::tuple_get_varchar;
use crate::storage::is_true;

pub fn get_sequences(machine: &mut Machine, database_name: &String) -> Vec<Sequence> {
    let mut sequences: Vec<Sequence> = Vec::new();

    let condition = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("database_name"))),
        Box::new(Expression::Const(RawVal::Str(database_name.clone())))
    );

    let columns = get_columns(machine, &SysDb::table_sequences());

    let tuples: Vec<Tuple> = read_tuples(machine, &SysDb::table_sequences())
        .into_iter()
        .filter(|tuple| is_true(&condition.result(tuple, &columns)))
        .collect();

    for elem in tuples.into_iter() {
        sequences.push(Sequence::new(tuple_get_varchar(&elem, 4).unwrap()));
    }

    return sequences;
}
