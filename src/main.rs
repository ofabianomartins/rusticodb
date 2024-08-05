use std::env;
use std::io;
use std::io::Write;

pub mod config;

pub mod parser;

pub mod connection;
pub mod table;
pub mod column;
pub mod index;
pub mod foreign_key;
pub mod database;

use connection::Connection;

fn main() {
    let mut args = env::args();
    if args.len() != 2 {
        eprintln!("Usage: prsqlite [rusticodb]");
        std::process::exit(1);
    }
    let file_path = args.nth(1).unwrap();
    let conn: Connection = Connection::load(&file_path);

    let mut stdout = io::stdout();
    let stdin = io::stdin();
    let mut buf = String::new();

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
