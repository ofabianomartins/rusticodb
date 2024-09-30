use std::io;
use std::io::Write;

pub mod storage;
pub mod parser;
pub mod setup;

use setup::setup_system;
use crate::parser::sql_executor::SqlExecutor;

fn main() {
    let mut stdout = io::stdout();
    let stdin = io::stdin();
    let mut buf = String::new();

    setup_system();
    let mut executor = SqlExecutor::new();

    loop {
        match &executor.actual_db {
            Some(actual_db) => print!("{} > ", actual_db),
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
                executor.parse_command(sql_command);
            }
        }
    }
}
