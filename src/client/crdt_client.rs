use std::net::SocketAddr;
use clap::{Parser};
use prost::Message as _;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[path = "../proto/mod.rs"]
mod proto;
#[path = "../common/mod.rs"]
mod common;

use proto::{proto_client_command, ProtoClientCommand};

#[derive(Parser, Debug)]
#[command(version, about = "CLI client for CRDT server operations")]
struct Cli {
    /// Address of the server's client listener.
    #[arg(long)]
    addr: String,
}

/// Parse a line of text into a server command, returning None for "quit".
fn parse_line(line: &str) -> Result<Option<proto_client_command::Command>, String> {
    let parts: Vec<&str> = line.trim().splitn(2, ' ').collect();
    match parts.as_slice() {
        ["add", value] => Ok(Some(proto_client_command::Command::Add(value.to_string()))),
        ["remove", value] => Ok(Some(proto_client_command::Command::Remove(value.to_string()))),
        ["print-state"] => Ok(Some(proto_client_command::Command::PrintState(true))),
        ["print-internals"] => Ok(Some(proto_client_command::Command::PrintInternals(true))),
        ["print-matrix"] => Ok(Some(proto_client_command::Command::PrintMatrix(true))),
        ["quit"] | ["exit"] => Ok(None),
        [""] | [] => Err(String::new()), // empty line, skip silently
        _ => Err(format!("Unknown command: {:?}. Try: add <v>, remove <v>, print-state, print-internals, print-matrix, quit", line.trim())),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let addr: SocketAddr = common::lookup(&cli.addr).await?;
    let stream = TcpStream::connect(addr).await?;
    let (read_half, mut write_half) = stream.into_split();
    let mut net_reader = BufReader::new(read_half);

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    eprintln!("Connected to {}. Enter commands (add, remove, print-state, print-internals, print-matrix, quit):", cli.addr);

    loop {
        // Prompt
        eprint!("> ");

        let line = match lines.next_line().await? {
            Some(l) => l,
            Option::None => break, // EOF (e.g. piped input exhausted or Ctrl-D)
        };

        let command = match parse_line(&line) {
            Ok(Some(cmd)) => cmd,
            Ok(Option::None) => break,     // quit
            Err(e) if e.is_empty() => continue, // blank line
            Err(e) => { eprintln!("{e}"); continue; }
        };

        let payload = ProtoClientCommand { command: Some(command) }.encode_to_vec();

        // Write length-prefixed frame.
        let len = payload.len() as u32;
        write_half.write_all(&len.to_be_bytes()).await?;
        write_half.write_all(&payload).await?;
        write_half.flush().await?;

        // Read length-prefixed response.
        let mut len_buf = [0u8; 4];
        net_reader.read_exact(&mut len_buf).await?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        net_reader.read_exact(&mut resp_buf).await?;

        println!("{}", String::from_utf8_lossy(&resp_buf));
    }

    eprintln!("Disconnected.");
    Ok(())
}
