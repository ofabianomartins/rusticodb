use sqlparser::ast::CreateTable;
use sqlparser::ast::Insert;

use sqlparser::ast::Statement;
use sqlparser::dialect::*;
use sqlparser::parser::Parser;

pub struct ExecutionCommands {
    pub create_tables: Vec<CreateTable>,
    pub inserts: Vec<Insert>
}

pub fn parser(sql: &str) -> ExecutionCommands {
    let dialect = GenericDialect {}; // or AnsiDialect
    
    let mut create_tables_vec: Vec<CreateTable> = Vec::new();
    let mut inserts_vec: Vec<Insert> = Vec::new();

    match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => {
            for val in statements {
                match val {
                    Statement::CreateTable(create_table_stmt) => {
                        create_tables_vec.push(create_table_stmt)
                    },
                    Statement::Insert(insert_stmt) => {
                        inserts_vec.push(insert_stmt)
                    },
                    other => {
                        println!("Not matched: {:?}", other)
                    }
                }
            }
        },
        Err(e) => {
            println!("Error during parsing: {e:?}");
        }
    }

    return ExecutionCommands { 
        create_tables: create_tables_vec,
        inserts: inserts_vec
    }
}
