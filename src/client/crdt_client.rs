use std::net::SocketAddr;
use clap::{Parser, Subcommand};
use prost::Message as _;
use rand::Rng;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use std::time::{Duration, Instant};

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
    mode: Mode,
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
        send_command(&mut write_half, command).await?;
        let resp = recv_response(&mut net_reader).await?;
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
    let mut latencies: Vec<Duration> = if let Some(req) = requests { Vec::with_capacity(req as usize) } else { Vec::new() };
    let mut count: u64 = 0;

    let wall_start = Instant::now();

    loop {
        if let Some(max_requests) = requests {
            if count >= max_requests {
                break;
            }
        }
        let key = rng.gen_range(0..key_space);
        let command = if rng.gen_bool(remove_chance) {
            proto_client_command::Command::RemoveRandom(true)
        } else {
            proto_client_command::Command::Add(format!("key-{key}"))
        };

        let t0 = Instant::now();
        send_command(&mut write_half, command).await?;
        recv_response(&mut net_reader).await?;
        latencies.push(t0.elapsed());
        count += 1;
        if sleep_ms > 0 {
            println!("Sleeping...");
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        }
    }

    let elapsed = wall_start.elapsed();

    // Compute percentiles on sorted latency list.
    latencies.sort_unstable();
    let pct = |p: f64| -> Duration {
        let idx = ((latencies.len() as f64 * p / 100.0).ceil() as usize)
            .saturating_sub(1)
            .min(latencies.len() - 1);
        latencies[idx]
    };
    let mean = latencies.iter().sum::<Duration>() / latencies.len() as u32;

    println!("requests:    {count}");
    println!("elapsed:     {:.3}s", elapsed.as_secs_f64());
    println!("throughput:  {:.1} req/s", count as f64 / elapsed.as_secs_f64());
    println!("latency mean:{:.3}ms", mean.as_secs_f64() * 1000.0);
    println!("latency p50: {:.3}ms", pct(50.0).as_secs_f64() * 1000.0);
    println!("latency p95: {:.3}ms", pct(95.0).as_secs_f64() * 1000.0);
    println!("latency p99: {:.3}ms", pct(99.0).as_secs_f64() * 1000.0);

    Ok(())
}

// ── Entry point ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let addr: SocketAddr = common::lookup(&cli.addr).await?;
    match cli.mode {
        Mode::Interactive => run_interactive(addr, &cli.addr).await?,
        Mode::Bench { requests, remove_chance, key_space, sleep_ms } =>
            run_bench(addr, requests, remove_chance, key_space, sleep_ms).await?,
    }
    Ok(())
}
