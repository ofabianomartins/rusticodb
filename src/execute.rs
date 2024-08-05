use std::fs::File;

use crate::parser::parser;
use crate::parser::ExecutionCommands;

use crate::file_engine::create_table;

pub fn execute(filepath: &String, sql: &String) {
    let _file = File::create(filepath);

    let commands: ExecutionCommands = parser(sql);

    for create_table_statement in commands.create_tables {
        create_table::create_table(filepath, create_table_statement)
    }
}
