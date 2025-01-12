use crate::config::Config;

use crate::machine::Column;
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::raw_val::RawVal;
use crate::machine::Condition;
use crate::machine::Condition2Type;
use crate::machine::get_columns;
use crate::machine::read_tuples;
use crate::machine::update_tuples;

use crate::storage::tuple::Tuple;

pub fn get_sequence_next_id(machine: &mut Machine, column: &Column) -> Option<u64> {
    let table_sequences = Table::new(
        Config::sysdb(),
        Config::sysdb_table_sequences()
    );
    let condition = Condition::Func2(
        Condition2Type::And,
        Box::new(Condition::Func2(
            Condition2Type::Equal,
            Box::new(Condition::ColName(String::from("database_name"))),
            Box::new(Condition::Const(RawVal::Str(column.database_name.clone())))
        )),
        Box::new(Condition::Func2(
            Condition2Type::And,
            Box::new(Condition::Func2(
                Condition2Type::Equal,
                Box::new(Condition::ColName(String::from("table_name"))),
                Box::new(Condition::Const(RawVal::Str(column.table_name.clone())))
            )),
            Box::new(Condition::Func2(
                Condition2Type::Equal,
                Box::new(Condition::ColName(String::from("column_name"))),
                Box::new(Condition::Const(RawVal::Str(column.name.clone())))
            ))
        ))
    );

    let columns = get_columns(machine, &table_sequences);

    let tuples: Vec<Tuple> = read_tuples(machine, &table_sequences)
        .into_iter()
        .filter(|tuple| condition.evaluate(tuple, &columns))
        .collect();

    let mut next_id: Option<u64> = None;

    for elem in tuples.into_iter() {
        let mut new_elem = elem.clone();
        let next_id_value = elem.get_unsigned_bigint(5).unwrap();

        new_elem.push_unsigned_bigint(next_id_value);
        update_tuples(machine, &table_sequences, &mut  vec![new_elem]);
        next_id = Some(elem.get_unsigned_bigint(5).unwrap());

        break;
    }

    return next_id;
}
