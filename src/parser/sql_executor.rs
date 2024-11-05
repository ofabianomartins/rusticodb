use sqlparser::ast::ObjectType;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::ast::Statement;

use crate::machine::column::Column;
use crate::machine::column::ColumnType;
use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::machine::result_set::ExecutionError;
use crate::machine::result_set::ResultSetType;
use crate::parser::parse_query::parse_query;
use crate::config::Config;

pub struct SqlExecutor {
}

impl SqlExecutor {

    pub fn new() -> Self {
        SqlExecutor { }
    }

    pub fn parse_command(
        &mut self,
        machine: &mut Machine,
        sql_command: &str
    ) -> Result<Vec<ResultSet>, ExecutionError> { 
        let dialect = GenericDialect {};

        match Parser::parse_sql(&dialect, sql_command) {
            Ok(commands) => {
                let mut result_sets: Vec<ResultSet> = Vec::new();
                for command in commands {
                    match self.process_command(machine, command) {
                        Ok(result_set) => {
                            result_sets.push(result_set);
                        },
                        Err(err) => {
                            return Err(err);
                        }
                    }
                }

                return Ok(result_sets);
            },
            Err(ParserError::ParserError(err)) => Err(ExecutionError::ParserError(err)),
            Err(ParserError::TokenizerError(err)) => Err(ExecutionError::TokenizerError(err)),
            Err(ParserError::RecursionLimitExceeded) => Err(ExecutionError::RecursionLimitExceeded)
        }
    }

    pub fn process_command(
        &mut self, 
        machine: &mut Machine,
        statement: Statement
    ) -> Result<ResultSet, ExecutionError> { 
        match statement {
            Statement::Use { db_name } => {
                machine.set_actual_database(db_name.to_string())
            },
            Statement::CreateDatabase { db_name, if_not_exists, location: _, managed_location: _ } => {
                machine.create_database(db_name.to_string(), if_not_exists)
            },
            Statement::CreateTable(create_table) => {
                if let Some(db_name) = machine.context.actual_database.clone() {
                    return machine.create_table(
                        &db_name,
                        &create_table.name.to_string(),
                        create_table.if_not_exists,
                        create_table.columns
                    );
                } else {
                    return Err(ExecutionError::DatabaseNotSetted);
                }
            },
            Statement::Drop { object_type, if_exists: _, names: _, cascade: _, restrict: _, purge: _, temporary: _ } => {
                match object_type {
                    ObjectType::Table => {
                        Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP TABLE")))
                    },
                    _ => todo!()
                }
            },
            Statement::Query(query) => {
                if let Some(db_name) = machine.context.actual_database.clone() {
                    let query_data = parse_query(query).unwrap();
                    let tuples = machine.read_tuples(&db_name, &query_data.table);
                    return Ok(ResultSet::new_select(query_data.select, tuples))
                } else {
                    return Err(ExecutionError::DatabaseNotSetted);
                }
            },
            Statement::ShowVariable { variable } => {
                if let Some(data) = variable.get(0) {
                    if data.value == "DATABASES" {
                        let db_name = Config::system_database();
                        let table_databases = Config::system_database_table_databases();
                        let mut columns: Vec<Column> = Vec::new();
                        columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));
                        let tuples = machine.read_tuples(&db_name, &table_databases);
                        return Ok(ResultSet::new_select(columns, tuples))
                    }
                }
                Err(ExecutionError::NotImplementedYet)
            },
            Statement::ShowTables { extended: _, full: _, db_name, filter: _ } => {
                let mut columns: Vec<Column> = Vec::new();
                columns.push(Column::new_column(String::from("database"), ColumnType::Varchar));
                columns.push(Column::new_column(String::from("name"), ColumnType::Varchar));
                let mut db_name_str: String = Config::system_database();
                let table_tables: String = Config::system_database_table_tables();
                if let Some(data) = db_name {
                    db_name_str = data.to_string();
                }
                let tuples = machine.read_tuples(&db_name_str, &table_tables);
                return Ok(ResultSet::new_select(columns, tuples))
            },
            value => { 
                println!("{:?}", value);
                Err(ExecutionError::NotImplementedYet)
            }
        }
    }

}
