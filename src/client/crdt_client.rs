use std::net::SocketAddr;

use clap::{Parser, Subcommand};
use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;


// Inline the proto types needed (same as the server).
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

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Insert a string into the OR-Set.
    Add {
        value: String,
    },
    /// Remove a string from the OR-Set.
    Remove {
        value: String,
    },
    /// Ask the server to print CRDT inner state.
    PrintState,
    PrintInternals,
    PrintMatrix,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let command = Some(match cli.command {
        Command::Add { value } => proto_client_command::Command::Add(value),
        Command::Remove { value } => proto_client_command::Command::Remove(value),
        Command::PrintState => proto_client_command::Command::PrintState(true),
        Command::PrintInternals => proto_client_command::Command::PrintInternals(true),
        Command::PrintMatrix => proto_client_command::Command::PrintMatrix(true),
    });
    let payload = ProtoClientCommand { command }.encode_to_vec();

    let addr: SocketAddr = common::lookup(&cli.addr).await?;

    let stream = TcpStream::connect(addr).await?;
    let (mut read_half, mut write_half) = stream.into_split();

    // Write 4-byte big-endian length prefix then payload.
    let len = payload.len() as u32;
    write_half.write_all(&len.to_be_bytes()).await?;
    write_half.write_all(&payload).await?;
    write_half.flush().await?;

    // Read 4-byte length prefix then response bytes.
    let mut len_buf = [0u8; 4];
    read_half.read_exact(&mut len_buf).await?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;
    let mut resp_buf = vec![0u8; resp_len];
    read_half.read_exact(&mut resp_buf).await?;
    let response = String::from_utf8_lossy(&resp_buf);
    println!("{response}");

    Ok(())
}
