use sqlparser::ast::CreateTable;
use sqlparser::ast::Insert;
use sqlparser::ast::Query;

use sqlparser::ast::Statement;
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

pub struct SQLCommands {
    pub create_tables: Vec<CreateTable>,
    pub inserts: Vec<Insert>
}

pub struct QueryCommands {
    pub queries: Vec<Box<Query>>
}

pub fn parse_command(sql: &str) -> SQLCommands {
    let dialect = SQLiteDialect {}; // or AnsiDialect
    
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

    return SQLCommands { 
        create_tables: create_tables_vec,
        inserts: inserts_vec
    }
}

pub fn parse_query(sql: &str) -> QueryCommands {
    let dialect = SQLiteDialect {}; // or AnsiDialect
    
    let mut queries_vec: Vec<Box<Query>> = Vec::new();

    match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => {
            for val in statements {
                match val {
                    Statement::Query(query_stmt) => {
                        queries_vec.push(query_stmt);
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

    return QueryCommands { 
        queries: queries_vec
    }
}
