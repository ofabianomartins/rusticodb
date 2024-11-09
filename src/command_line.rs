use std::io;
use std::io::Write;

use crate::parser::sql_executor::SqlExecutor;
use crate::machine::machine::Machine;

pub fn command_line(machine: &mut Machine) {

    let mut stdout = io::stdout();
    let stdin = io::stdin();
    let mut buf = String::new();

    let mut executor = SqlExecutor::new();

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
                match executor.parse_command(machine, sql_command) {
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
