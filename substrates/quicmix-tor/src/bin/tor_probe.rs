//! tor_probe — measure a real Tor circuit via the **native C Tor daemon** (no arti),
//! using quicmix's SOCKS5 client. Launches and supervises its own `tor`.
//!
//! Run: `cargo run --bin tor_probe -- [host:port]`  (default check.torproject.org:80)

use anyhow::Result;
use quicmix::tor::{socks5_connect, TorDaemon};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<()> {
    let target = std::env::args().nth(1).unwrap_or_else(|| "check.torproject.org:80".to_string());
    let (host, port) = target.rsplit_once(':').unwrap_or((target.as_str(), "80"));
    let port: u16 = port.parse().unwrap_or(80);

    eprintln!("launching native tor…");
    let t0 = Instant::now();
    let tor = TorDaemon::launch().await?;
    let boot = t0.elapsed();
    eprintln!("bootstrapped in {:.1}s (socks {}); opening circuit to {target}…", boot.as_secs_f64(), tor.socks);

    let t1 = Instant::now();
    let mut stream = socks5_connect(tor.socks, host, port).await?;
    let connect = t1.elapsed();

    let req = format!("GET / HTTP/1.0\r\nHost: {host}\r\n\r\n");
    let t2 = Instant::now();
    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;
    let mut buf = [0u8; 256];
    let n = stream.read(&mut buf).await?;
    let rtt = t2.elapsed();

    println!("real Tor (native daemon) → {target}  [via quicmix::tor::socks5_connect]:");
    println!("  bootstrap:      {:.1} s", boot.as_secs_f64());
    println!("  stream connect: {:.0} ms", connect.as_secs_f64() * 1e3);
    println!("  first-byte RTT: {:.0} ms ({n} bytes)", rtt.as_secs_f64() * 1e3);
    println!("  response head:  {:?}", String::from_utf8_lossy(&buf[..n.min(48)]));
    Ok(())
}
