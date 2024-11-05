use std::io;
use std::io::Write;

pub mod storage;
pub mod parser;
pub mod setup;
pub mod machine;
pub mod config;
pub mod utils;

use crate::setup::setup_system;
use crate::parser::sql_executor::SqlExecutor;
use crate::machine::machine::Machine;
use crate::machine::context::Context;
use crate::storage::pager::Pager;

fn main() {
    let mut stdout = io::stdout();
    let stdin = io::stdin();
    let mut buf = String::new();

    let pager = Pager::new();
    let context = Context::new();
    let mut machine = Machine::new(pager, context);

    let mut executor = SqlExecutor::new();

    setup_system(&mut machine);

    loop {
        match &machine.context.actual_database {
            Some(database_name) => print!("{} > ", database_name),
            None => print!("<no-database> > ")
        }
        stdout.flush().expect("flush stdout");
        buf.truncate(0);
        let n = stdin.read_line(&mut buf).expect("read line");
        let line = &buf[..n];
        match line.trim() {
            "quit" | "exit" => break,
            "" => continue,
            sql_command => {
                match executor.parse_command(&mut machine, sql_command) {
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
}
