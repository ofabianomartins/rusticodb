pub mod storage;
pub mod parser;
pub mod setup;
pub mod machine;
pub mod config;
pub mod utils;

use std::io;
use std::io::Write;

use crate::setup::setup_system;
use crate::machine::machine::Machine;
use crate::machine::context::Context;
use crate::storage::pager::Pager;

use crate::parser::sql_executor::SqlExecutor;

fn main() {
    let pager = Pager::new();
    let context = Context::new();
    let mut machine = Machine::new(pager, context);

    setup_system(&mut machine);

    let mut stdout = io::stdout();
    let stdin = io::stdin();
    let mut buf = String::new();

    let mut executor = SqlExecutor::new(machine);

    loop {
        print!("{} > ", executor.get_database_name());
        stdout.flush().expect("flush stdout");
        buf.truncate(0);
        let n = stdin.read_line(&mut buf).expect("read line");
        let line = &buf[..n];
        match line.trim() {
            "quit" | "exit" => break,
            "" => continue,
            sql_command => {
                match executor.parse_command(sql_command) {
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
