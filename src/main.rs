use std::io;
use std::io::Write;

pub mod storage;

fn main() {
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
