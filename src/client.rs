use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt};

use std::io;
use std::io::Write;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server_addr = "127.0.0.1:8080";

    let mut stdout = io::stdout();
    let stdin = io::stdin();
    let mut buf = String::new();

    // Connect to the server
    let mut stream = TcpStream::connect(server_addr).await?;
    println!("Connected to server at {}", server_addr);

    loop {
        print!("shell > ");
        stdout.flush().expect("flush stdout");
        buf.truncate(0);
        let n = stdin.read_line(&mut buf).expect("read line");
        let line = &buf[..n];
        stream.write_all(line.as_bytes()).await?;
        println!("Sent: {}", line);

        // Buffer to store response
        let mut buffer = [0; 1024];

        // Read server response
        let bytes_read = stream.read(&mut buffer).await?;
        if bytes_read == 0 {
            println!("Connection closed by server.");
            break;
        }

        let response = String::from_utf8_lossy(&buffer[..bytes_read]);
        println!("Received from server: {}", response);
    }

    Ok(())
}

