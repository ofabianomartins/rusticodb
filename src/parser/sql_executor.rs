use sqlparser::ast::ObjectType;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::ast::Statement;

use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::utils::execution_error::ExecutionError;

use crate::parser::use_database::use_database;
use crate::parser::show_databases::show_databases;
use crate::parser::create_database::create_database;
use crate::parser::drop_database::drop_database;
use crate::parser::show_tables::show_tables;
use crate::parser::create_table::create_table;
use crate::parser::drop_table::drop_table;
use crate::parser::query::query;

pub struct SqlExecutor {
    pub machine: Machine
}

impl SqlExecutor {

    pub fn new(machine: Machine) -> Self {
        SqlExecutor { machine }
    }

    pub fn get_database_name(&self) -> String {
        match &self.machine.context.actual_database {
            Some(database_name) => database_name.clone(),
            None => String::from("<no-database>")
        }
    }

    pub fn parse_command(&mut self, sql_command: &str) -> Result<Vec<ResultSet>, ExecutionError> { 
        let dialect = GenericDialect {};

        match Parser::parse_sql(&dialect, sql_command) {
            Ok(commands) => {
                let mut result_sets: Vec<ResultSet> = Vec::new();
                for command in commands {
                    match self.process_command(command) {
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

    pub fn process_command(&mut self, statement: Statement) -> Result<ResultSet, ExecutionError> { 
        match statement {
            Statement::Use(statement) => use_database(&mut self.machine, statement),
            Statement::CreateDatabase { db_name, if_not_exists, .. } => {
                create_database(&mut self.machine, db_name.to_string(), if_not_exists)
            },
            Statement::CreateTable(statement) => create_table(&mut self.machine, statement),
            Statement::Drop { object_type, if_exists, names, .. } => {
                match object_type {
                    ObjectType::Database => drop_database(&mut self.machine, names, if_exists),
                    ObjectType::Table => drop_table(&mut self.machine, names, if_exists),
                    value => { 
                        println!("DROP {:?}", value);
                        Err(ExecutionError::NotImplementedYet)
                    }
                }
            },
            Statement::Query(statement) => query(&mut self.machine, statement),
            Statement::ShowDatabases { .. } => show_databases(&mut self.machine),
            Statement::ShowTables { .. } => show_tables(&mut self.machine),
            value => { 
                println!("{:?}", value);
                Err(ExecutionError::NotImplementedYet)
            }
        }
    }

}
