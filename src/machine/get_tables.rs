use crate::config::Config;

use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::raw_val::RawVal;
use crate::machine::Condition;
use crate::machine::Condition2Type;
use crate::machine::get_columns;
use crate::machine::read_tuples;

use crate::storage::tuple::Tuple;

pub fn get_tables(machine: &mut Machine, database_name: &String) -> Vec<Table> {
    let mut tables: Vec<Table> = Vec::new();

    let table_tables = Table::new(
        Config::sysdb(),
        Config::sysdb_table_tables()
    );

    let condition = Condition::Func2(
        Condition2Type::Equal,
        Box::new(Condition::ColName(String::from("database_name"))),
        Box::new(Condition::Const(RawVal::Str(database_name.clone())))
    );

    let columns = get_columns(machine, &table_tables);

    let tuples: Vec<Tuple> = read_tuples(machine, &table_tables)
        .into_iter()
        .filter(|tuple| condition.evaluate(tuple, &columns))
        .collect();

    for elem in tuples.into_iter() {
        tables.push(
            Table::new_with_alias(
                database_name.clone(),
                database_name.clone(),
                elem.get_string(2).unwrap(),
                elem.get_string(2).unwrap()
            )
        );
    }

    return tables;
}
