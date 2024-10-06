use sqlparser::ast::ObjectType;
use sqlparser::dialect::GenericDialect;

use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;

use sqlparser::ast::Statement;

use crate::machine::machine::Machine;
use crate::machine::context::Context;

pub struct SqlExecutor {
}

impl SqlExecutor {

    pub fn new() -> Self {
        SqlExecutor { }
    }

    pub fn parse_command(&mut self, context: &mut Context, machine: &mut Machine, sql_command: &str) {
        let dialect = GenericDialect {};

        self.process_commands(context, machine, Parser::parse_sql(&dialect, sql_command))
    }

    pub fn process_commands(&mut self, context: &mut Context, machine: &mut Machine, statements: Result<Vec<Statement>, ParserError>) { 
        match statements {
            Ok(commands) => {
                for command in commands {
                    self.process_command(context, machine, command);
                }
            },
            Err(ParserError::ParserError(err)) => println!("ParserError: {}", err),
            Err(ParserError::TokenizerError(err)) => println!("TokenError: {}", err),
            Err(ParserError::RecursionLimitExceeded) => println!("RecursionLimitExceeded! ")
        }
    }

    pub fn process_command(&mut self, context: &mut Context, machine: &mut Machine, statement: Statement) { 
        match statement {
            Statement::Use { db_name } => {
                context.set_actual_database(db_name.to_string());
            },
            Statement::CreateDatabase { db_name, if_not_exists: _, location: _, managed_location: _ } => {
                machine.create_database(&db_name.to_string());
                context.add_database(db_name.to_string());

            },
            Statement::CreateTable(create_table) => {
                if let Some(db_name) = context.actual_database.clone() {
                   context.add_table(db_name.to_string(), create_table.name.to_string());
                   machine.create_table(&db_name, &create_table.name.to_string());
                } else {
                   println!("Database not setted!")
                }
            },
            Statement::Drop { object_type, if_exists: _, names: _, cascade: _, restrict: _, purge: _, temporary: _ } => {
                match object_type {
                    ObjectType::Table => {
                        
                    },
                    _ => todo!()
                }

            },
            _ => todo!()
        }
    }

}
