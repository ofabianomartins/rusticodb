use crate::machine::Column;
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::read_tuples;
use crate::machine::get_columns_table_definition;
use crate::machine::map_column_type;

use crate::storage::Data;
use crate::storage::Expression;
use crate::storage::Expression2Type;
use crate::storage::Tuple;

use crate::config::SysDb;

pub fn get_columns(machine: &mut Machine, table: &Table) -> Vec<Column> {
    let condition = Expression::Func2(
        Expression2Type::And,
        Box::new(Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("database_name"))),
            Box::new(Expression::Const(Data::Varchar(table.database_name.clone())))
        )),
        Box::new(Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("table_name"))),
            Box::new(Expression::Const(Data::Varchar(table.name.clone())))
        ))
    );

    let column_names: Vec<String> = get_columns_table_definition().iter().map(|e| e.name.clone()).collect();

    let tuples: Vec<Tuple> = read_tuples(machine, &SysDb::table_columns())
        .into_iter()
        .filter(|tuple| condition.result(tuple, &column_names).is_true())
        .collect();

    let mut columns: Vec<Column> = Vec::new();

    for elem in tuples.into_iter() {
        let Data::UnsignedBigint(id) = elem.get(0).unwrap() else { todo!()};
        let Data::Boolean(not_null) = elem.get(5).unwrap() else { todo!() };
        let Data::Boolean(unique) = elem.get(6).unwrap() else { todo!() };
        let Data::Boolean(primary_key) = elem.get(7).unwrap() else { todo!() };
        let Data::Varchar(default) = elem.get(8).unwrap() else { todo!() };

        columns.push(
            Column::new_with_alias(
                *id,
                table.database_name.clone(),
                table.database_alias.clone(),
                table.name.clone(),
                table.alias.clone(),
                elem.get(3).unwrap().to_string(),
                elem.get(3).unwrap().to_string(),
                map_column_type(elem.get(4).unwrap().to_string()),
                *not_null,
                *unique,
                *primary_key,
                default.clone()
            )
        );
    }

    return columns;
}
