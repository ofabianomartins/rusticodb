pub mod storage;
pub mod parser;
pub mod setup;
pub mod machine;
pub mod config;
pub mod utils;
pub mod sys_db;

use std::str;

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::error::Error;

use crate::setup::setup_system;
use crate::machine::Machine;
use crate::storage::pager::Pager;

use crate::parser::sql_executor::SqlExecutor;

async fn handle_client(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut buffer = [0; 1024];

    // Read data from the client
    let bytes_read = stream.read(&mut buffer).await?;
    if bytes_read == 0 {
        return Ok(());
    }

    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    setup_system(&mut machine);

    let mut executor = SqlExecutor::new(machine);

    let received_data = match str::from_utf8(&buffer[..bytes_read]) {
        Ok(v) => v,
        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    print!("Received data: {}", received_data);

    match executor.parse_command(received_data) {
        Ok(result_set) => {
            for item in result_set {
                println!("{}", item);
                stream.write_all(format!("{}\n", item).as_bytes()).await?;
            }
        },
        Err(err) => {
            println!("{:?}", err);
            stream.write_all(format!("{:?}\n", err).as_bytes()).await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Server listening on 127.0.0.1:8080");

    loop {
        // Accept an incoming connection
        let (stream, addr) = listener.accept().await?;
        println!("New client connected: {}", addr);

        // Spawn a new task to handle the client
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream).await {
                eprintln!("Error handling client: {}", e);
            }
        });
    }
}

