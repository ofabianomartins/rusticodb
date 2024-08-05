use std::fs::create_dir_all;
use std::fs::File;

use crate::config::Config;
use crate::parser::parser;
use crate::parser::ExecutionCommands;

mod create_table;

fn test_data_file_exists(config: &Config) {
    let _ = create_dir_all(config.base_path.clone());
    let _ = File::create(format!("{0}/data.json", config.base_path.clone()));
}

pub fn execute(config: &Config, filepath: &str, sql: &str) {
    test_data_file_exists(config);

    let _file = File::create(filepath);

    let commands: ExecutionCommands = parser(sql);

    for create_table_statement in commands.create_tables {
        create_table::create_table(config, create_table_statement)
    }
}
