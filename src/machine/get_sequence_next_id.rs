use crate::machine::Attribution;
use crate::machine::Column;
use crate::machine::Machine;
use crate::machine::get_columns;
use crate::machine::read_tuples;
use crate::machine::update_row;
use crate::machine::get_sequences_next_id_column_definition;

use crate::storage::RawVal;
use crate::storage::Expression;
use crate::storage::Expression2Type;
use crate::storage::Tuple;
use crate::storage::tuple_push_unsigned_bigint;
use crate::storage::tuple_get_unsigned_bigint;
use crate::storage::is_true;

use crate::config::SysDb;

pub fn get_sequence_next_id(machine: &mut Machine, column: &Column) -> Option<u64> {
    let condition = Expression::Func2(
        Expression2Type::And,
        Box::new(Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("database_name"))),
            Box::new(Expression::Const(RawVal::Str(column.database_name.clone())))
        )),
        Box::new(Expression::Func2(
            Expression2Type::And,
            Box::new(Expression::Func2(
                Expression2Type::Equal,
                Box::new(Expression::ColName(String::from("table_name"))),
                Box::new(Expression::Const(RawVal::Str(column.table_name.clone())))
            )),
            Box::new(Expression::Func2(
                Expression2Type::Equal,
                Box::new(Expression::ColName(String::from("column_name"))),
                Box::new(Expression::Const(RawVal::Str(column.name.clone())))
            ))
        ))
    );

    let columns = get_columns(machine, &SysDb::table_sequences()).iter().map(|e| e.name.clone()).collect();
    let tuples: Vec<Tuple> = read_tuples(machine, &SysDb::table_sequences())
        .into_iter()
        .filter(|tuple| is_true(&condition.result(tuple, &columns)))
        .collect();

    let mut next_id: Option<u64> = None;

    for elem in tuples.into_iter() {
        let mut new_elem = elem.clone();
        let next_id_value = tuple_get_unsigned_bigint(&elem, 5).unwrap();

        let column: Column = get_sequences_next_id_column_definition().get(0).unwrap().clone();
        let expression = Expression::Const(RawVal::Int(next_id_value + 1));
        let attribution = Attribution::new(column, expression);

        let selection = Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("id"))),
            Box::new(Expression::Const(RawVal::Int(tuple_get_unsigned_bigint(&elem, 0).unwrap())))
        );

        tuple_push_unsigned_bigint(&mut new_elem, next_id_value);
        let _ = update_row(machine, &SysDb::table_sequences(), &mut vec![attribution], selection);
        next_id = Some(tuple_get_unsigned_bigint(&elem, 5).unwrap());

        break;
    }

    return next_id;
}
