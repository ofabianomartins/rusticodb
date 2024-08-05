use std::fs::File;

use crate::config::Config;
use crate::parser::parser;
use crate::parser::ExecutionCommands;

use crate::file_engine::create_table;
use crate::file_engine::helpers;

pub fn execute(config: &Config, filepath: &str, sql: &str) {
    helpers::create_base_folder(config);

    let _file = File::create(filepath);

    let commands: ExecutionCommands = parser(sql);

    for create_table_statement in commands.create_tables {
        create_table::create_table(config, create_table_statement)
    }
}
