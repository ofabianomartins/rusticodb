use sqlparser::ast::CreateTable;
use sqlparser::ast::Statement;

use sqlparser::dialect::*;
use sqlparser::parser::Parser;

use std::fs::File;
use std::fs::create_dir_all;

fn test_data_file_exists() {
    let basepath = "/tmp/rusticodb/main";
    let _ = create_dir_all(basepath);
    let _ = File::create(format!("{basepath}/data.json"));
}

pub fn create_table(statement: CreateTable) {
    let basepath = "/tmp/rusticodb/main";
    let table_path = format!("{basepath}/tables/{0}", statement.name);

    let _ = create_dir_all(table_path.clone());
    let _ = File::create(format!("{table_path}/metadata.json"));
}

pub fn execute(filepath: &str, sql: &str) {
    test_data_file_exists();

    let file = File::create(filepath);

    let dialect = GenericDialect {}; // or AnsiDialect
    let parse_result = Parser::parse_sql(&dialect, sql);

    // println!("Struct: {:?}", parse_result.clone().unwrap());
    match parse_result {
        Ok(statements) => {
            for val in statements {
                match val {
                    Statement::CreateTable(create_table_stmt) => {
                        create_table(create_table_stmt)
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
}
