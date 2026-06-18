//! quicmix node demo — **one binary, two roles, no flag**.
//!
//! Runs two instances of the *same* `quicmix::node::Node` type:
//! - node **B** acts as a **gateway** (egresses to the origin), and
//! - node **A** acts as a local **ingress** proxy, carrying traffic over the mix
//!   substrate to B's gateway.
//!
//! They are the identical binary/type with no role flag — a peer is just another
//! quicmix node — and both use the same oracle-fed transport, which is what makes
//! congestion control work across the mix. A self-test fetches the origin through
//! the whole path.
//!
//! Run: `cargo run --manifest-path quicmix/Cargo.toml --bin quicmix`

use anyhow::Result;
use quicmix::client::bdp_bytes;
use quicmix::emulator::EmulatedMixnet;
use quicmix::node::Node;
use quicmix::relay::start_relay;
use quicmix::OracleParams;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    // Origin HTTP server (the "website").
    let origin = TcpListener::bind("127.0.0.1:0").await?;
    let origin_addr = origin.local_addr()?;
    tokio::spawn(async move {
        while let Ok((mut s, _)) = origin.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let _ = s.read(&mut buf).await;
                let body = "hello from origin via quicmix\n";
                let resp =
                    format!("HTTP/1.0 200 OK\r\nContent-Length: {}\r\n\r\n{body}", body.len());
                let _ = s.write_all(resp.as_bytes()).await;
            });
        }
    });

    // The measured mix model (oracle) — both nodes are tuned to it.
    let p = OracleParams {
        hops: 3,
        mean_hop_delay: Duration::from_millis(5),
        drop_prob: 0.01,
        slot_interval: Duration::from_millis(1),
        mtu: 1200,
    };

    // Two instances of the SAME node type — no "client" vs "gateway" binary.
    let node_b = Node::new(p)?; // will act as gateway for A
    let node_a = Node::new(p)?; // will act as ingress for the local app

    // B exposes its gateway. (Every quicmix node does this, always.)
    let (_b_ep, b_gateway_addr) = node_b.serve_gateway().await?;

    // Mix substrate (emulator) between A and B's gateway.
    let buf = ((bdp_bytes(&p) / p.mtu as u64) as usize).max(16);
    let relay = start_relay(
        b_gateway_addr,
        std::sync::Arc::new(EmulatedMixnet::with_queue(p, buf)),
        std::sync::Arc::new(EmulatedMixnet::with_queue(p, buf)),
    )
    .await?;
    let front = relay.front;

    // A connects to B's gateway through the mix (session-warmed link), then runs
    // its local ingress proxy over that link. Same transport on both ends.
    let link = node_a.connect(front, node_b.cert()).await?;
    let proxy_addr = node_a.serve_ingress("127.0.0.1:0", link.conn.clone()).await?;
    let _keep_alive = link.endpoint; // keep A's client endpoint alive

    println!("one binary, two roles:");
    println!("  node A (ingress proxy): http://{proxy_addr}");
    println!("  node B (gateway):       {b_gateway_addr}");
    println!("  origin:                 http://{origin_addr}");
    println!("  path: app → A(ingress) → QUIC(quicmix CC) → mix-emulator → B(gateway) → origin\n");

    // Self-test: app uses A's proxy (CONNECT) to reach the origin via B.
    let start = Instant::now();
    let mut s = TcpStream::connect(proxy_addr).await?;
    s.write_all(format!("CONNECT {origin_addr} HTTP/1.1\r\nHost: {origin_addr}\r\n\r\n").as_bytes())
        .await?;
    let head = read_until_blank(&mut s).await?;
    let tunnel_ok = head.contains("200");
    s.write_all(format!("GET / HTTP/1.0\r\nHost: {origin_addr}\r\n\r\n").as_bytes())
        .await?;
    s.shutdown().await.ok();
    let mut resp = Vec::new();
    s.read_to_end(&mut resp).await?;
    let elapsed = start.elapsed();
    let body_ok = String::from_utf8_lossy(&resp).contains("hello from origin via quicmix");

    println!("CONNECT tunnel:        {}", if tunnel_ok { "established" } else { "FAILED" });
    println!(
        "end-to-end (A→mix→B):  {} ({} bytes, {:.0} ms)",
        if body_ok { "OK" } else { "FAILED" },
        resp.len(),
        elapsed.as_secs_f64() * 1e3
    );
    println!("origin response:       {:?}", String::from_utf8_lossy(&resp));
    let m = relay.up.metrics();
    println!(
        "substrate boundary:    sent={} recv={} send_errors={} dropped={} queue_depth={}",
        m.sent, m.received, m.send_errors, m.dropped, m.queue_depth
    );

    use std::io::Write;
    std::io::stdout().flush().ok();
    if !(tunnel_ok && body_ok) {
        anyhow::bail!("round-trip failed");
    }
    if std::env::var("QUICMIX_SERVE").is_ok() {
        println!("\nserving — Ctrl-C to exit. curl -x http://{proxy_addr} http://{origin_addr}/");
        tokio::signal::ctrl_c().await.ok();
    }
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
