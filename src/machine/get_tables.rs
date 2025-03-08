use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::raw_val::RawVal;
use crate::machine::get_columns;
use crate::machine::read_tuples;
use crate::machine::Expression;
use crate::machine::Expression2Type;

use crate::config::SysDb;

use crate::storage::Tuple;

pub fn get_tables(machine: &mut Machine, database_name: &String) -> Vec<Table> {
    let mut tables: Vec<Table> = Vec::new();

    let condition = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("database_name"))),
        Box::new(Expression::Const(RawVal::Str(database_name.clone())))
    );

    let columns = get_columns(machine, &SysDb::table_tables());

    let tuples: Vec<Tuple> = read_tuples(machine, &SysDb::table_tables())
        .into_iter()
        .filter(|tuple| condition.result(tuple, &columns).is_true())
        .collect();

    for elem in tuples.into_iter() {
        tables.push(
            Table::new_with_alias(
                database_name.clone(),
                database_name.clone(),
                elem.get_varchar(2).unwrap(),
                elem.get_varchar(2).unwrap()
            )
        );
    }

    return tables;
}
