//! torprobe — measure a real Tor circuit via arti, through quicmix's
//! `StreamSubstrate` trait (so the trait is exercised against the real network).
//!
//! Run: `cargo run --release --manifest-path quicmix/torprobe/Cargo.toml -- [host:port]`
//! If arti complains about state-dir permissions on a restricted host, set
//! `FS_MISTRUST_DISABLE_PERMISSIONS_CHECKS=1` and a writable `HOME`.

use anyhow::Result;
use arti_client::{TorClient, TorClientConfig};
use quicmix::tor::{Stream, StreamSubstrate};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tor_rtcompat::PreferredRuntime;

/// Real `StreamSubstrate` backed by a bootstrapped arti Tor client.
struct TorSub(Arc<TorClient<PreferredRuntime>>);

#[async_trait::async_trait]
impl StreamSubstrate for TorSub {
    async fn open(&self, target: &str) -> std::io::Result<Box<dyn Stream>> {
        let s = self
            .0
            .connect(target)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(Box::new(s))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let target = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "check.torproject.org:80".to_string());
    let host = target.split(':').next().unwrap_or("").to_string();

    eprintln!("bootstrapping Tor (arti)…");
    let t0 = Instant::now();
    let tor = TorClient::create_bootstrapped(TorClientConfig::default()).await?;
    let boot = t0.elapsed();
    eprintln!("bootstrapped in {:.1}s; opening circuit to {target}…", boot.as_secs_f64());

    let sub = TorSub(tor);
    let t1 = Instant::now();
    let mut stream = sub.open(&target).await?;
    let connect = t1.elapsed();

    let req = format!("GET / HTTP/1.0\r\nHost: {host}\r\n\r\n");
    let t2 = Instant::now();
    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;
    let mut buf = [0u8; 256];
    let n = stream.read(&mut buf).await?;
    let rtt = t2.elapsed();

    println!("real Tor (arti) → {target}  [via quicmix::tor::StreamSubstrate]:");
    println!("  bootstrap:      {:.1} s", boot.as_secs_f64());
    println!("  stream connect: {:.0} ms", connect.as_secs_f64() * 1e3);
    println!("  first-byte RTT: {:.0} ms ({n} bytes)", rtt.as_secs_f64() * 1e3);
    println!("  response head:  {:?}", String::from_utf8_lossy(&buf[..n.min(48)]));
    Ok(())
}
