use std::io;
use std::io::Write;

pub mod storage;
pub mod parser;
pub mod setup;
pub mod machine;
pub mod config;


use crate::setup::setup_system;
use crate::parser::sql_executor::SqlExecutor;
use crate::machine::machine::Machine;
use crate::machine::context::Context;

fn main() {
    let mut stdout = io::stdout();
    let stdin = io::stdin();
    let mut buf = String::new();

    let mut machine = Machine::new();

    let mut context = Context::new();
    let mut executor = SqlExecutor::new();

    setup_system(&mut context, &mut machine);

    loop {
        match &context.actual_database {
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
                executor.parse_command(&mut context, sql_command);
            }
        }
    }
}
