use crate::config::Config;

use crate::machine::Column;
use crate::machine::ColumnType;
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::raw_val::RawVal;
use crate::machine::Condition;
use crate::machine::Condition2Type;
use crate::machine::read_tuples;

use crate::storage::Tuple;

pub fn get_columns_table_definition() -> Vec<Column> {
    return vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("database_name"),
            ColumnType::Varchar,
            true,
            false,
            false
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("table_name"),
            ColumnType::Varchar,
            true,
            false,
            false
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("name"),
            ColumnType::Varchar,
            true,
            false,
            false
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("type"),
            ColumnType::Varchar,
            true,
            false,
            false
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("not_null"),
            ColumnType::Boolean,
            true,
            false,
            false
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("primary_key"),
            ColumnType::Boolean,
            true,
            false,
            false
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("unique"),
            ColumnType::Boolean,
            true,
            false,
            false

        ),
    ];
}

pub fn get_columns(machine: &mut Machine, table: &Table) -> Vec<Column> {
    let table_columns = Table::new(
        Config::sysdb(),
        Config::sysdb_table_columns()
    );

    let condition = Condition::Func2(
        Condition2Type::And,
        Box::new(Condition::Func2(
            Condition2Type::Equal,
            Box::new(Condition::ColName(String::from("database_name"))),
            Box::new(Condition::Const(RawVal::Str(table.database_name.clone())))
        )),
        Box::new(Condition::Func2(
            Condition2Type::Equal,
            Box::new(Condition::ColName(String::from("table_name"))),
            Box::new(Condition::Const(RawVal::Str(table.name.clone())))
        ))
    );

    let tuples: Vec<Tuple> = read_tuples(machine, &table_columns)
        .into_iter()
        .filter(|tuple| condition.evaluate(tuple, &get_columns_table_definition()))
        .collect();

    let mut columns: Vec<Column> = Vec::new();

    for elem in tuples.into_iter() {
        columns.push(
            Column::new_with_alias(
                table.database_name.clone(),
                table.database_alias.clone(),
                table.name.clone(),
                table.alias.clone(),
                elem.get_string(3).unwrap(),
                elem.get_string(3).unwrap(),
                ColumnType::Varchar,
                elem.get_boolean(5).unwrap(),
                elem.get_boolean(6).unwrap(),
                elem.get_boolean(7).unwrap(),
            )
        );
    }

    return columns;
}
