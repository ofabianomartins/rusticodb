pub mod storage;
pub mod parser;
pub mod setup;
pub mod machine;
pub mod config;
pub mod utils;

use std::io;
use std::io::Write;

use crate::setup::setup_system;
use crate::machine::Machine;
use crate::storage::Pager;
use crate::storage::pager_new;

use crate::parser::parse_command;

fn main() {
    let pager: Pager = pager_new();
    let mut machine = Machine::new(pager);

    setup_system(&mut machine);

    let mut stdout = io::stdout();
    let stdin = io::stdin();
    let mut buf = String::new();

    // let previous_comands: Vec<String> = Vec::new();

    loop {
        print!("{} > ", machine.get_actual_database_name());
        stdout.flush().expect("flush stdout");
        buf.truncate(0);
        let n = stdin.read_line(&mut buf).expect("read line");
        let line = &buf[..n];
        match line.trim() {
            "quit" | "exit" => break,
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
}
