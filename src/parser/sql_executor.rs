use sqlparser::dialect::GenericDialect;

use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;

use sqlparser::ast::Statement;

use crate::storage::pager::Pager;

pub struct SqlExecutor {
    pub actual_db: Option<String>
}

impl SqlExecutor {
    pub fn new() -> Self {
        SqlExecutor {
            actual_db: None
        }
    }

    pub fn parse_command(&mut self, sql_command: &str) {
        let dialect = GenericDialect {};

        self.process_commands(Parser::parse_sql(&dialect, sql_command))
    }

    pub fn process_commands(&mut self, statements: Result<Vec<Statement>, ParserError>) { 
        match statements {
            Ok(commands) => {
                for command in commands {
                    self.process_command(command);
                }
            },
            Err(ParserError::ParserError(err)) => println!("ParserError: {}", err),
            Err(ParserError::TokenizerError(err)) => println!("TokenError: {}", err),
            Err(ParserError::RecursionLimitExceeded) => println!("RecursionLimitExceeded! ")
        }
    }

    pub fn process_command(&mut self, statement: Statement) { 
        match statement {
            Statement::Use { db_name } => {
                self.actual_db = Some(db_name.to_string());
            },
            Statement::CreateDatabase { db_name, if_not_exists: _, location: _, managed_location: _ } => {
                let mut pager = Pager::new();
                pager.create_database(&db_name.to_string());
            },
            Statement::CreateTable(create_table) => {
                let mut pager = Pager::new();
                match &self.actual_db {
                    Some(db_name) => {
                        pager.create_file(&db_name, &create_table.name.to_string());
                    },
                    None => println!("Database not setted!")
                }
            },
            _ => todo!()
        }
    }

}
