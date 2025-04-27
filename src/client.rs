
use std::net::TcpStream;
use std::io::{self, Write, Read};
use std::thread;
use std::error::Error;

use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server_addr = "127.0.0.1:8080";

    const HISTORY_FILE: &str = ".sql_shell_history"; // history file

    // let previous_comands: Vec<String> = Vec::new();
    let mut rl = DefaultEditor::new().expect("Failed to create editor");

    // Try to load history file
    if Path::new(HISTORY_FILE).exists() {
        if rl.load_history(HISTORY_FILE).is_ok() {
            println!("✅ History loaded from {}", HISTORY_FILE);
        }
    }

    let mut stream = TcpStream::connect(server_addr).expect("Failed to connect to server");
    stream.set_nonblocking(true).expect("Cannot set non-blocking");

    println!("✅ Connected! Type SQL commands. Type ':exit' to quit.");

    let mut stream_clone = stream.try_clone().expect("Failed to clone stream");

    // Thread to listen for server messages
    thread::spawn(move || {
        let mut buffer = [0; 512];
        loop {
            match stream_clone.read(&mut buffer) {
                Ok(0) => {
                }
                Ok(n) => {
                    if n > 0 {
                        let msg = String::from_utf8_lossy(&buffer[..n]);
                        println!("\n📬 Server says: {}", msg.trim());
                        print!("sql> ");
                        io::stdout().flush().unwrap();
                    }
                }
                Err(_) => {
                    // No new data, just wait a bit
                    thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        }
    });

    loop {
        match rl.readline("sql > ") {
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
                        stream.write_all(sql_command.as_bytes()).expect("Failed to send SQL command");
                        println!("Sent: {}", line);
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

    Ok(())
}

