use std::io;
use std::io::Write;

pub mod storage;
pub mod parser;

use crate::parser::sql_executor::SqlExecutor;

fn main() {
    let mut stdout = io::stdout();
    let stdin = io::stdin();
    let mut buf = String::new();

    let mut executor = SqlExecutor::new();

    executor.parse_command("CREATE DATABASE rusticodb");
    executor.parse_command("CREATE TABLE tables");
    executor.parse_command("USE rusticodb");
    executor.parse_command("CREATE TABLE tables");

    loop {
        print!("rusticodb> ");
        stdout.flush().expect("flush stdout");
        buf.truncate(0);
        let n = stdin.read_line(&mut buf).expect("read line");
        let line = &buf[..n];
        match line.trim() {
            "quit" | "exit" => break,
            "" => continue,
            line => {
                println!("{}", line);
            }
        }
    }
}
