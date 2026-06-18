//! nym_e2e — **real QUIC over the real Nym mainnet**, end-to-end (T2).
//!
//! This is the wiring the emulator demo (`quicmix/src/bin/quicmix.rs`) never had:
//! the same two-`Node` path (ingress A → mix → gateway B → origin), but the "mix"
//! is the **live Nym mixnet**, not `EmulatedMixnet`. quicmix's oracle-fed CC is
//! tuned to the **measured** Nym `OracleParams` (from `realprobe`).
//!
//!   app → Node A ingress → quinn(quicmix CC) ─UDP→ client bridge ─Nym(SURBs)→
//!         [ Nym mainnet ] → gateway → quinn server (Node B) → origin → … → back
//!
//! Run (open egress required): `cargo run --release -p quicmix-nym --bin nym_e2e`
//! Two Nym clients bootstrap to mainnet first (~10–30 s each), then a single HTTP
//! fetch is driven through the whole path. Expect a slow but successful round-trip
//! (Nym mainnet RTT ≈ 2.8 s, measured 0% loss).

use anyhow::Result;
use quicmix::node::Node;
use quicmix::OracleParams;
use quicmix_nym::{spawn_client_bridge, NymGateway, NymSubstrate};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Measured Nym-mainnet oracle (from `realprobe`: p50 RTT ≈ 2.8 s → mean_hop_delay
/// ≈ 470 ms over 3 hops, ~0% loss, ~6 msg/s → slot ≈ 158 ms). These are the CC
/// parameters the scheduler consumes for the Nym substrate.
fn nym_oracle() -> OracleParams {
    OracleParams {
        hops: 3,
        mean_hop_delay: Duration::from_millis(470),
        drop_prob: 0.0,
        slot_interval: Duration::from_millis(158),
        mtu: 1200,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let p = nym_oracle();
    eprintln!(
        "oracle (measured Nym): hops={} mean_hop_delay={:?} drop={} slot={:?} rtt≈{:?}",
        p.hops, p.mean_hop_delay, p.drop_prob, p.slot_interval, p.rtt()
    );

    // ---- Origin "website" (local TCP server the gateway egresses to).
    let origin = TcpListener::bind("127.0.0.1:0").await?;
    let origin_addr = origin.local_addr()?;
    tokio::spawn(async move {
        while let Ok((mut s, _)) = origin.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let _ = s.read(&mut buf).await;
                let body = "hello from origin via real Nym + quicmix\n";
                let resp =
                    format!("HTTP/1.0 200 OK\r\nContent-Length: {}\r\n\r\n{body}", body.len());
                let _ = s.write_all(resp.as_bytes()).await;
            });
        }
    });

    // ---- Gateway side: Node B's quinn server + a Nym client bridging into it.
    let node_b = Node::new(p)?;
    let (_b_ep, server_udp) = node_b.serve_gateway().await?;
    eprintln!("bootstrapping gateway Nym client to mainnet…");
    let gw = NymGateway::connect().await?;
    let gw_addr = gw.nym_address().to_string();
    eprintln!("gateway Nym address: {gw_addr}");
    tokio::spawn(async move {
        if let Err(e) = gw.run(server_udp).await {
            eprintln!("gateway loop ended: {e:#}");
        }
    });

    // ---- Client side: a Nym substrate targeting the gateway, behind a UDP bridge.
    eprintln!("bootstrapping client Nym substrate to mainnet…");
    let surbs: u32 = std::env::var("NYM_SURBS").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    let sub = Arc::new(NymSubstrate::connect(&gw_addr, surbs, p).await?);
    let (front, bridge) = spawn_client_bridge(sub).await?;
    eprintln!("client bridge front (quinn connects here): {front}");

    // ---- Node A opens a quicmix QUIC connection to B *through the live mix*.
    let node_a = Node::new(p)?;
    eprintln!("QUIC handshake over real Nym (this takes a few mix RTTs)…");
    let t_hs = Instant::now();
    let link = node_a.connect(front, node_b.cert()).await?;
    eprintln!("QUIC connection established over Nym in {:.1} s", t_hs.elapsed().as_secs_f64());
    let proxy_addr = node_a.serve_ingress("127.0.0.1:0", link.conn.clone()).await?;
    let _keep_alive = link.endpoint;

    println!("\nlive path: app → A(ingress {proxy_addr}) → QUIC/quicmix → REAL Nym → B(gateway) → origin {origin_addr}\n");

    // ---- Self-test: fetch the origin through the whole real-Nym path.
    let start = Instant::now();
    let fetch = async {
        let mut s = TcpStream::connect(proxy_addr).await?;
        s.write_all(
            format!("CONNECT {origin_addr} HTTP/1.1\r\nHost: {origin_addr}\r\n\r\n").as_bytes(),
        )
        .await?;
        let head = read_until_blank(&mut s).await?;
        let tunnel_ok = head.contains("200");
        s.write_all(format!("GET / HTTP/1.0\r\nHost: {origin_addr}\r\n\r\n").as_bytes())
            .await?;
        s.shutdown().await.ok();
        let mut resp = Vec::new();
        s.read_to_end(&mut resp).await?;
        Ok::<_, anyhow::Error>((tunnel_ok, resp))
    };
    let (tunnel_ok, resp) = tokio::time::timeout(Duration::from_secs(180), fetch)
        .await
        .map_err(|_| anyhow::anyhow!("fetch timed out after 180s over Nym"))??;
    let elapsed = start.elapsed();
    let body_ok = String::from_utf8_lossy(&resp).contains("hello from origin via real Nym");

    println!("CONNECT tunnel:        {}", if tunnel_ok { "established" } else { "FAILED" });
    println!(
        "end-to-end (A→Nym→B):  {} ({} bytes, {:.1} s)",
        if body_ok { "OK" } else { "FAILED" },
        resp.len(),
        elapsed.as_secs_f64()
    );
    println!("origin response:       {:?}", String::from_utf8_lossy(&resp));
    let m = bridge.metrics();
    println!(
        "substrate boundary:    sent={} recv={} send_errors={} dropped={} queue_depth={}",
        m.sent, m.received, m.send_errors, m.dropped, m.queue_depth
    );

    use std::io::Write;
    std::io::stdout().flush().ok();
    if !(tunnel_ok && body_ok) {
        anyhow::bail!("round-trip over real Nym failed");
    }
    println!("\n✅ real QUIC + quicmix CC carried an HTTP fetch over the live Nym mainnet.");
    Ok(())
}

async fn read_until_blank(s: &mut TcpStream) -> Result<String> {
    let mut out = Vec::new();
    let mut b = [0u8; 1];
    while s.read_exact(&mut b).await.is_ok() {
        out.push(b[0]);
        if out.ends_with(b"\r\n\r\n") || out.len() > 8192 {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}
