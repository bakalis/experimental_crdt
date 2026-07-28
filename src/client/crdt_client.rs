use std::net::SocketAddr;
use clap::{Parser, Subcommand};
use prost::Message as _;
use rand::Rng;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use std::time::{Duration};

#[path = "../proto/mod.rs"]
mod proto;
#[path = "../common/mod.rs"]
mod common;
#[path = "../logging/mod.rs"]
mod logging;

use proto::{proto_client_command, ProtoClientCommand};

#[derive(Parser, Debug)]
#[command(version, about = "CLI client for CRDT server operations")]
struct Cli {
    /// Address of the server's client listener.
    #[arg(long)]
    addr: String,
    #[command(subcommand)]
    mode: Mode,
    #[arg(long, env = "METRICS_FILE_PATH")]
    metrics_path: String,
}

#[derive(Subcommand, Debug)]
enum Mode {
    /// Interactive REPL mode.
    Interactive,
    /// Benchmark mode: send random requests and print a summary.
    Bench {
        /// Total number of requests to send.
        #[arg(long)]
        requests: Option<u64>,

        #[arg(long)]
        sleep_ms: u64,

        /// Probability [0.0, 1.0] that each request is a remove instead of an add.
        #[arg(long, default_value_t = 0.3)]
        remove_chance: f64,
        /// Number of distinct values to use in the workload (acts as the key-space).
        #[arg(long, default_value_t = 1000000000)]
        key_space: u64,
    },
    OneRequestPerServer {
        #[arg(long)]
        num_gc_servers: u64,
        #[arg(long)]
        num_normal_servers: u64,
        #[arg(long, default_value_t = 1000000000)]
        key_space: u64,
    },
}

// ── Interactive helpers ──────────────────────────────────────────────────────

fn parse_line(line: &str) -> Result<Option<proto_client_command::Command>, String> {
    let parts: Vec<&str> = line.trim().splitn(2, ' ').collect();
    match parts.as_slice() {
        ["add", value]      => Ok(Some(proto_client_command::Command::Add(value.to_string()))),
        ["remove", value]   => Ok(Some(proto_client_command::Command::Remove(value.to_string()))),
        ["remove-random"]     => Ok(Some(proto_client_command::Command::RemoveRandom(true))),
        ["print-state"]     => Ok(Some(proto_client_command::Command::PrintState(true))),
        ["print-internals"] => Ok(Some(proto_client_command::Command::PrintInternals(true))),
        ["print-matrix"]    => Ok(Some(proto_client_command::Command::PrintMatrix(true))),
        ["quit"] | ["exit"] => Ok(None),
        [""] | []           => Err(String::new()),
        _ => Err(format!(
            "Unknown command: {:?}. Try: add <v>, remove <v>, print-state, print-internals, print-matrix, quit",
            line.trim()
        )),
    }
}

// ── Shared framing helpers ───────────────────────────────────────────────────

async fn send_command<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    command: proto_client_command::Command,
) -> anyhow::Result<()> {
    let payload = ProtoClientCommand { command: Some(command) }.encode_to_vec();
    let len = payload.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

async fn recv_response<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> anyhow::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; resp_len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

// ── Modes ────────────────────────────────────────────────────────────────────

async fn run_interactive(addr: SocketAddr, addr_str: &str) -> anyhow::Result<()> {
    let stream = TcpStream::connect(addr).await?;
    let (read_half, mut write_half) = stream.into_split();
    let mut net_reader = BufReader::new(read_half);
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    eprintln!("Connected to {addr_str}. Commands: add, remove, print-state, print-internals, print-matrix, quit");

    loop {
        eprint!("> ");
        let line = match lines.next_line().await? {
            Some(l) => l,
            Option::None    => break,
        };
        let command = match parse_line(&line) {
            Ok(Some(cmd)) => cmd,
            Ok(Option::None)      => break,
            Err(e) if e.is_empty() => continue,
            Err(e) => { eprintln!("{e}"); continue; }
        };
        let start_millis = std::time::Instant::now();
        send_command(&mut write_half, command).await?;
        let resp = recv_response(&mut net_reader).await?;
        metric!(event = "interactive_command", command = line, duration_millis = start_millis.elapsed().as_millis() as u64);
        println!("{}", String::from_utf8_lossy(&resp));
    }

    eprintln!("Disconnected.");
    Ok(())
}

async fn run_bench(
    addr: SocketAddr,
    requests: Option<u64>,
    remove_chance: f64,
    key_space: u64,
    sleep_ms: u64,
) -> anyhow::Result<()> {
    let stream = TcpStream::connect(addr).await?;
    let (read_half, mut write_half) = stream.into_split();
    let mut net_reader = BufReader::new(read_half);
    let mut rng = rand::thread_rng();
    let mut count: u64 = 0;

    loop {
        if let Some(max_requests) = requests {
            if count >= max_requests {
                break;
            }
        }
        let key = rng.gen_range(0..key_space);
        let (str_command, command) = if count != 0 && rng.gen_bool(remove_chance) {
            ("remove-random".to_string(), proto_client_command::Command::RemoveRandom(true))
        } else {
            let cmd_key = format!("key-{key}");
            (cmd_key.clone(), proto_client_command::Command::Add(cmd_key))
        };

        let start_millis = std::time::Instant::now();
        send_command(&mut write_half, command).await?;
        recv_response(&mut net_reader).await?;
        metric!(event = "bench_command", command = str_command, duration_millis = start_millis.elapsed().as_millis() as u64);
        count += 1;
        if sleep_ms > 0 {
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        }
    }
    Ok(())
}

async fn run_one_request_per_server(
    num_gc_servers: u64,
    num_normal_servers: u64,
    key_space: u64,
) -> anyhow::Result<()> {
    let mut rng = rand::thread_rng();
    println!("Sending one request to each of {num_gc_servers} GC servers and {num_normal_servers} normal servers.");

    for i in 1..num_gc_servers + 1 {
        let lookup_addr = format!("gc-server-{}:9100", i);
        println!("Looking up address for {lookup_addr}...");
        let stream = TcpStream::connect(lookup_addr).await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut net_reader = BufReader::new(read_half);
        let key = format!("gc-key-{}", rng.gen_range(0..key_space));
        let command = proto_client_command::Command::Add(key.clone());
        send_command(&mut write_half, command).await?;
        let resp = recv_response(&mut net_reader).await?;
        println!("{}", String::from_utf8_lossy(&resp));
    }

    for i in 1..num_normal_servers + 1 {
        let lookup_addr = format!("normal-server-{}:9100", i);
        println!("Looking up address for {lookup_addr}...");
        let stream = TcpStream::connect(lookup_addr).await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut net_reader = BufReader::new(read_half);
        let key = format!("normal-key-{}", rng.gen_range(0..key_space));
        let command = proto_client_command::Command::Add(key.clone());
        send_command(&mut write_half, command).await?;
        let resp = recv_response(&mut net_reader).await?;
        println!("{}", String::from_utf8_lossy(&resp));
    }

    Ok(())
}
// ── Entry point ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    logging::initialize_logging(cli.metrics_path.clone());
    let addr: SocketAddr = common::lookup(&cli.addr).await?;
    match cli.mode {
        Mode::Interactive => run_interactive(addr, &cli.addr).await?,
        Mode::Bench { requests, remove_chance, key_space, sleep_ms } =>
            run_bench(addr, requests, remove_chance, key_space, sleep_ms).await?,
        Mode::OneRequestPerServer { num_gc_servers, num_normal_servers, key_space } =>
            run_one_request_per_server(num_gc_servers, num_normal_servers, key_space).await?,
    }
    Ok(())
}
