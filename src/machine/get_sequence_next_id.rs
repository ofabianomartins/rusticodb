use crate::machine::Attribution;
use crate::machine::Column;
use crate::machine::Machine;
use crate::machine::get_columns;
use crate::machine::read_tuples;
use crate::machine::update_row;
use crate::machine::get_sequences_next_id_column_definition;

use crate::storage::Data;
use crate::storage::Expression;
use crate::storage::Expression2Type;
use crate::storage::Tuple;

use crate::config::SysDb;

pub fn get_sequence_next_id(machine: &mut Machine, column: &Column) -> Option<u64> {
    let condition = Expression::Func2(
        Expression2Type::And,
        Box::new(Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("database_name"))),
            Box::new(Expression::Const(Data::Varchar(column.database_name.clone())))
        )),
        Box::new(Expression::Func2(
            Expression2Type::And,
            Box::new(Expression::Func2(
                Expression2Type::Equal,
                Box::new(Expression::ColName(String::from("table_name"))),
                Box::new(Expression::Const(Data::Varchar(column.table_name.clone())))
            )),
            Box::new(Expression::Func2(
                Expression2Type::Equal,
                Box::new(Expression::ColName(String::from("column_name"))),
                Box::new(Expression::Const(Data::Varchar(column.name.clone())))
            ))
        ))
    );

    let columns = get_columns(machine, &SysDb::table_sequences()).iter().map(|e| e.name.clone()).collect();
    let tuples: Vec<Tuple> = read_tuples(machine, &SysDb::table_sequences())
        .into_iter()
        .filter(|tuple| condition.result(tuple, &columns).is_true())
        .collect();

    let mut next_id: Option<u64> = None;

    for elem in tuples.into_iter() {
        let mut new_elem = elem.clone();
        let Data::UnsignedBigint(next_id_value) = elem.get(5).unwrap() else { todo!() };

        let column: Column = get_sequences_next_id_column_definition().get(0).unwrap().clone();
        let expression = Expression::Const(Data::UnsignedBigint(next_id_value + 1));
        let attribution = Attribution::new(column, expression);

        let selection = Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("id"))),
            Box::new(Expression::Const(elem.get(0).unwrap().clone()))
        );

        new_elem.push(Data::UnsignedBigint(*next_id_value)); 
        let _ = update_row(machine, &SysDb::table_sequences(), &mut vec![attribution], selection);
        next_id = Some(*next_id_value);

        break;
    }

    return next_id;
}
