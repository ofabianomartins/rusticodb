use crate::machine::Column;
use crate::machine::ColumnType;
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::raw_val::RawVal;
use crate::machine::read_tuples;
use crate::machine::get_columns_table_definition;
use crate::machine::Expression;
use crate::machine::Expression2Type;

use crate::storage::Tuple;
use crate::sys_db::SysDb;

pub fn get_columns(machine: &mut Machine, table: &Table) -> Vec<Column> {
    let condition = Expression::Func2(
        Expression2Type::And,
        Box::new(Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("database_name"))),
            Box::new(Expression::Const(RawVal::Str(table.database_name.clone())))
        )),
        Box::new(Expression::Func2(
            Expression2Type::Equal,
            Box::new(Expression::ColName(String::from("table_name"))),
            Box::new(Expression::Const(RawVal::Str(table.name.clone())))
        ))
    );

    let tuples: Vec<Tuple> = read_tuples(machine, &SysDb::table_columns())
        .into_iter()
        .filter(|tuple| condition.result(tuple, &get_columns_table_definition()).is_true())
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
                match elem.get_string(4).unwrap().as_str() {
                    "UNSIGNED TINYINT" => ColumnType::UnsignedTinyint,
                    "SIGNED TINYINT" => ColumnType::SignedTinyint,
                    "UNSIGNED SMALLINT" => ColumnType::UnsignedSmallint,
                    "SIGNED SMALLINT" => ColumnType::SignedSmallint,
                    "UNSIGNED INT" => ColumnType::UnsignedInt,
                    "SIGNED INT" => ColumnType::SignedInt,
                    "UNSIGNED BIGINT" => ColumnType::UnsignedBigint,
                    "SIGNED BIGINT" => ColumnType::SignedBigint,
                    "VARCHAR" => ColumnType::Varchar,
                    "TEXT" => ColumnType::Text,
                    _ => ColumnType::Varchar
                },
                elem.get_boolean(5).unwrap(),
                elem.get_boolean(6).unwrap(),
                elem.get_boolean(7).unwrap(),
                elem.get_string(8).unwrap()
            )
        );
    }

    return columns;
}
