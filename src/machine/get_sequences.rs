use crate::machine::Sequence;
use crate::machine::Machine;
use crate::machine::raw_val::RawVal;
use crate::machine::Condition;
use crate::machine::Condition2Type;
use crate::machine::get_columns;
use crate::machine::read_tuples;

use crate::sys_db::SysDb;

use crate::storage::Tuple;

pub fn get_sequences(machine: &mut Machine, database_name: &String) -> Vec<Sequence> {
    let mut sequences: Vec<Sequence> = Vec::new();

    let condition = Condition::Func2(
        Condition2Type::Equal,
        Box::new(Condition::ColName(String::from("database_name"))),
        Box::new(Condition::Const(RawVal::Str(database_name.clone())))
    );

    let columns = get_columns(machine, &SysDb::table_sequences());

    let tuples: Vec<Tuple> = read_tuples(machine, &SysDb::table_sequences())
        .into_iter()
        .filter(|tuple| condition.evaluate(tuple, &columns))
        .collect();

    for elem in tuples.into_iter() {
        sequences.push(Sequence::new(elem.get_string(4).unwrap()));
    }

    return sequences;
}
