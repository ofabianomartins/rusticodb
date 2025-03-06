
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;

use crate::parser::process_command;
use crate::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::utils::ExecutionError;

pub fn parse_command(machine: &mut Machine, sql_command: &str) -> Result<Vec<ResultSet>, ExecutionError> { 
    let dialect = PostgreSqlDialect {};

    match Parser::parse_sql(&dialect, sql_command) {
        Ok(commands) => {
            let mut result_sets: Vec<ResultSet> = Vec::new();
            for command in commands {
                match process_command(machine, command) {
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

