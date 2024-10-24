use sqlparser::ast::ObjectType;
use sqlparser::dialect::GenericDialect;

use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;

use sqlparser::ast::Statement;

use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::machine::result_set::ExecutionError;
use crate::machine::result_set::ResultSetType;

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
            Statement::Query(box_query) => {
                Ok(ResultSet::new_select(Vec::new(), Vec::new()))
            },
            _ => todo!()
        }
    }

}
