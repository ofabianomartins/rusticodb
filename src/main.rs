pub mod storage;
pub mod parser;
pub mod setup;
pub mod machine;
pub mod config;
pub mod utils;

use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::path::Path;

use crate::setup::setup_system;

use crate::machine::Machine;
use crate::machine::PagerManager;
use crate::machine::pager_manager_new;

use crate::parser::parse_command;

fn main() {
    let pager: PagerManager = pager_manager_new();
    let mut machine = Machine::new(pager);

    setup_system(&mut machine);

    const HISTORY_FILE: &str = ".sql_shell_history"; // history file

    // let previous_comands: Vec<String> = Vec::new();
    let mut rl = DefaultEditor::new().expect("Failed to create editor");

    // Try to load history file
    if Path::new(HISTORY_FILE).exists() {
        if rl.load_history(HISTORY_FILE).is_ok() {
            println!("✅ History loaded from {}", HISTORY_FILE);
        }
    }

    loop {
        match rl.readline(&format!("{} > ", machine.get_actual_database_name()).to_string()) {
            Ok(line) => {
                let command = line.trim();

                // Save the command to history
                let _ = rl.add_history_entry(command);

                // Echo the SQL command back (simulate processing)
                match command {
                    "quit" | "exit" =>{
                        println!("👋 Exiting SQL Shell. Goodbye!");
                        // Save history before exiting
                        rl.append_history(HISTORY_FILE).expect("Failed to save history");
                        break;
                    },
                    "" => continue,
                    sql_command => {
                        match parse_command(&mut machine, sql_command) {
                            Ok(result_set) => {
                                for item in result_set {
                                    println!("{}", item);
                                }
                            },
                            Err(err) => println!("{:?}", err)
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C pressed. Exiting...");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D pressed. Exiting...");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
}
