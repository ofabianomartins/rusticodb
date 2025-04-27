use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::get_columns;
use crate::machine::read_tuples;

use crate::config::SysDb;

use crate::storage::Data;
use crate::storage::Expression;
use crate::storage::Expression2Type;
use crate::storage::Tuple;

pub fn get_tables(machine: &mut Machine, database_name: &String) -> Vec<Table> {
    let mut tables: Vec<Table> = Vec::new();

    let condition = Expression::Func2(
        Expression2Type::Equal,
        Box::new(Expression::ColName(String::from("database_name"))),
        Box::new(Expression::Const(Data::Varchar(database_name.clone())))
    );

    let columns = get_columns(machine, &SysDb::table_tables()).iter().map(|e| e.name.clone()).collect();

    let tuples: Vec<Tuple> = read_tuples(machine, &SysDb::table_tables())
        .into_iter()
        .filter(|tuple| condition.result(tuple, &columns).is_true())
        .collect();

    for elem in tuples.into_iter() {
        tables.push(
            Table::new_with_alias(
                database_name.clone(),
                database_name.clone(),
                elem.get(2).unwrap().to_string(),
                elem.get(2).unwrap().to_string()
            )
        );
    }

    return tables;
}
