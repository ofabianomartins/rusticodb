use sqlparser::ast::CreateTable;
use sqlparser::ast::ColumnOption;
use sqlparser::ast::DataType;
use sqlparser::ast::Expr;
use sqlparser::ast::Value;
use sqlparser::ast::UnaryOperator;

use crate::machine::Machine;
use crate::machine::Column;
use crate::machine::ColumnType;
use crate::machine::Table;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::create_table as machine_create_table;
use crate::machine::check_table_exists;

use crate::utils::ExecutionError;

pub fn create_table(machine: &mut Machine, create_table: CreateTable) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let if_not_exists = create_table.if_not_exists;
        let table = Table::new(db_name, create_table.name.to_string());

        if check_table_exists(machine, &table) && if_not_exists {
            return Ok(
                ResultSet::new_command(
                    ResultSetType::Change, String::from("CREATE TABLE")
                )
            );
        }
        if check_table_exists(machine, &table) {
            return Err(ExecutionError::DatabaseExists(table.database_name.to_string()));
        }

        let mut columns: Vec<Column> = Vec::new();

        for column in create_table.columns.iter() {
            let mut notnull_column: bool = false;
            let mut unique_column: bool = false;
            let mut primary_key: bool = false;
            let mut default: String = String::from("");

            for option in &column.options {
                match &option.option {
                    ColumnOption::NotNull => { notnull_column = true }
                    ColumnOption::Unique { is_primary, ..} => {
                        notnull_column = true;
                        unique_column = true;
                        primary_key = *is_primary;
                    }
                    ColumnOption::Default(Expr::Value(Value::Boolean(true))) => {
                        default = String::from("1")
                    }
                    ColumnOption::Default(Expr::Value(Value::Boolean(false))) => {
                        default = String::from("0")
                    }
                    ColumnOption::Default(Expr::Value(Value::Number(default_value, _))) => {
                        default = String::from(default_value)
                    }
                    ColumnOption::Default(Expr::Value(Value::SingleQuotedString(default_value))) => {
                        default = default_value.clone()
                    }
                    ColumnOption::Default(Expr::UnaryOp { op: UnaryOperator::Minus, expr }) => {
                        default = format!("-{}", expr.to_string())
                    }
                    other => {
                        println!(" create table default other {:?}", other);
                    }
                }
            }

            let type_column = match column.data_type {
                DataType::TinyInt(_) => if primary_key { ColumnType::UnsignedTinyint } else { ColumnType::SignedTinyint },
                DataType::UnsignedTinyInt(_) => ColumnType::UnsignedTinyint,
                
                DataType::SmallInt(_) => if primary_key { ColumnType::UnsignedSmallint } else { ColumnType::SignedSmallint },
                DataType::UnsignedSmallInt(_) => ColumnType::UnsignedSmallint,

                DataType::MediumInt(_) => if primary_key { ColumnType::UnsignedInt } else { ColumnType::SignedInt },
                DataType::UnsignedMediumInt(_) => ColumnType::UnsignedInt,

                DataType::Int(_) => if primary_key { ColumnType::UnsignedInt } else { ColumnType::SignedInt },
                DataType::UnsignedInt(_) => ColumnType::UnsignedInt,

                DataType::Integer(_) => if primary_key { ColumnType::UnsignedInt } else { ColumnType::SignedInt },
                DataType::UnsignedInteger(_) => ColumnType::UnsignedInt,

                DataType::BigInt(_) => if primary_key { ColumnType::UnsignedBigint } else { ColumnType::SignedBigint },
                DataType::UnsignedBigInt(_) => ColumnType::UnsignedBigint,

                DataType::Varchar(_) => ColumnType::Varchar,
                DataType::Text => ColumnType::Text,
                DataType::Boolean => ColumnType::UnsignedTinyint,
                _ => ColumnType::Undefined
            };

            let tcolumn = Column::new(
                table.database_name.clone(),
                table.name.clone(),
                column.name.to_string().clone(),
                type_column,
                notnull_column,
                unique_column,
                primary_key,
                default
            );
            columns.push(tcolumn);
        }

        return machine_create_table(machine, &table, columns);
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

