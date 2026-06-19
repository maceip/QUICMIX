//! Live HOPR probe — run on a host with a hoprd node to VERIFY the v4 binding.
//!
//! Usage:
//!   hopr_probe <api_url> <token> <dest_address> <target host:port> [hops] [data_host] [listen_host]
//!
//! Opens a real v4 UDP session to DEST (an on-chain Address) tunnelling to TARGET (a
//! udp echo the exit node can reach), sends a datagram in, and waits for it to come
//! back — proving the session carries datagrams round-trip through the mixnet.

use quicmix::MixTransport;
use quicmix_hopr::{hopr_oracle, HoprSubstrate, SessionOpts};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 5 {
        eprintln!("usage: hopr_probe <api_url> <token> <dest_address> <target host:port> [hops] [data_host]");
        std::process::exit(2);
    }
    let (api, token, dest, target) = (a[1].clone(), a[2].clone(), a[3].clone(), a[4].clone());
    let hops: u8 = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(0);
    let data_host = a.get(6).cloned();
    let listen_host = a.get(7).cloned().unwrap_or_else(|| "127.0.0.1:0".into());

    let oracle = hopr_oracle(hops);
    let opts = SessionOpts { hops, target: target.clone(), data_host, listen_host, ..Default::default() };
    println!("opening hopr v4 session → dest {dest} via target {target} ({hops} hops)…");
    let hopr = HoprSubstrate::connect(api, token, dest, opts, oracle).await?;
    let (ip, port) = hopr.session_endpoint();
    println!("session up: node data socket {ip}:{port}");

    let payload = b"quicmix-hopr-probe".to_vec();
    let t = Instant::now();
    hopr.try_send(payload.clone()).await?;
    println!("sent {} bytes into the session; awaiting echo…", payload.len());

    let result = tokio::time::timeout(Duration::from_secs(30), hopr.try_recv()).await;
    hopr.close().await;
    match result {
        Ok(Ok(got)) => {
            println!(
                "ROUND-TRIP OK in {:.0} ms: {:?}",
                t.elapsed().as_secs_f64() * 1e3,
                String::from_utf8_lossy(&got)
            );
            Ok(())
        }
        Ok(Err(e)) => anyhow::bail!("session recv error: {e}"),
        Err(_) => anyhow::bail!("no echo within 30s (check the target echo, hops, and open channels)"),
    }
}
