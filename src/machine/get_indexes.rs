use crate::machine::Index;
use crate::machine::Machine;
use crate::machine::get_columns;
use crate::machine::read_tuples;

use crate::config::SysDb;

use crate::storage::Data;
use crate::storage::Expression;
use crate::storage::Expression2Type;
use crate::storage::Tuple;

pub fn get_indexes(machine: &mut Machine, database_name: &String) -> Vec<Index> {
    let mut indexes: Vec<Index> = Vec::new();

    let condition = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("database_name"))),
        Box::new(Expression::Const(Data::Varchar(database_name.clone())))
    );

    let columns = get_columns(machine, &SysDb::table_indexes()).iter().map(|e| e.name.clone()).collect();

    let tuples: Vec<Tuple> = read_tuples(machine, &SysDb::table_indexes())
        .into_iter()
        .filter(|tuple| condition.result(tuple, &columns).is_true())
        .collect();

    for elem in tuples.into_iter() {
        indexes.push(Index::new(elem.get(4).unwrap().clone().to_string()));
    }

    return indexes;
}
