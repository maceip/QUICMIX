//! Pooled multipath proxy demo — the client http→quic proxy wired to all three
//! quicmix mechanisms and exercised under concurrency.
//!
//! Shows: app → **pooled ingress** → quic(**oracle-fed CC**) → **Striped**
//! round-robin substrate → gateway → origin. The pool pre-warms circuits, serves
//! requests round-robin across them, and **rotates** (retire-after-N-uses) so it
//! stays unlinkable and heals — all off the hot path.
//!
//! Run: `cargo run --manifest-path quicmix/Cargo.toml --bin proxy [requests]`

use anyhow::Result;
use quicmix::client::Congestion;
use quicmix::emulator::EmulatedMixnet;
use quicmix::node::Node;
use quicmix::proxy::{PoolConfig, WarmPool};
use quicmix::relay::start_relay;
use quicmix::rotation::FrontFactory;
use quicmix::striped::Striped;
use quicmix::{MixTransport, OracleParams};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A front factory whose substrate is a [`Striped`] round-robin of two emulated
/// mixnets — datagram-level multipath *within* each pooled circuit.
fn striped_front(gateway: SocketAddr, p: OracleParams, buf: usize) -> FrontFactory {
    let mk = move || -> Arc<dyn MixTransport> {
        Arc::new(Striped::new(vec![
            Arc::new(EmulatedMixnet::with_queue(p, buf)) as Arc<dyn MixTransport>,
            Arc::new(EmulatedMixnet::with_queue(p, buf)),
        ]))
    };
    Arc::new(move || {
        let (up, down) = (mk(), mk());
        Box::pin(async move { start_relay(gateway, up, down).await.map(|r| r.front).map_err(Into::into) })
    })
}

async fn fetch_once(proxy: SocketAddr, origin: SocketAddr) -> Result<bool> {
    let mut s = TcpStream::connect(proxy).await?;
    s.write_all(format!("CONNECT {origin} HTTP/1.1\r\nHost: {origin}\r\n\r\n").as_bytes())
        .await?;
    // read the proxy's CONNECT response line(s) up to blank.
    let mut head = Vec::new();
    let mut b = [0u8; 1];
    while s.read_exact(&mut b).await.is_ok() {
        head.push(b[0]);
        if head.ends_with(b"\r\n\r\n") || head.len() > 4096 {
            break;
        }
    }
    s.write_all(format!("GET / HTTP/1.0\r\nHost: {origin}\r\n\r\n").as_bytes())
        .await?;
    s.shutdown().await.ok();
    let mut resp = Vec::new();
    s.read_to_end(&mut resp).await?;
    Ok(String::from_utf8_lossy(&resp).contains("hello via pooled quicmix proxy"))
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider().install_default().ok();
    let requests: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(24);

    // Measured/known mix model — both the CC transport and the substrate use it.
    let p = OracleParams {
        hops: 3,
        mean_hop_delay: Duration::from_millis(5),
        drop_prob: 0.01,
        slot_interval: Duration::from_millis(1),
        mtu: 1200,
    };
    let buf = ((quicmix::client::bdp_bytes(&p) / p.mtu as u64) as usize).max(16);

    // Origin "website".
    let origin = TcpListener::bind("127.0.0.1:0").await?;
    let origin_addr = origin.local_addr()?;
    tokio::spawn(async move {
        while let Ok((mut s, _)) = origin.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = s.read(&mut buf).await;
                let body = "hello via pooled quicmix proxy\n";
                let _ = s
                    .write_all(
                        format!("HTTP/1.0 200 OK\r\nContent-Length: {}\r\n\r\n{body}", body.len())
                            .as_bytes(),
                    )
                    .await;
            });
        }
    });

    // Gateway node.
    let node_b = Node::new(p)?;
    let (_b_ep, gw) = node_b.serve_gateway().await?;

    // The wiring: oracle-fed CC transport + two striped substrate front factories
    // (so circuits — and thus requests — round-robin across substrates) + rotation.
    let transport = Some(Congestion::Quicmix.transport(&p)); // CC + oracle
    let fronts = vec![striped_front(gw, p, buf), striped_front(gw, p, buf)];
    let cfg = PoolConfig {
        target: 4,
        max_uses: Some(3), // rotate each circuit after 3 requests → unlinkable + heals
        max_age: None,
        tick: Duration::from_millis(100),
        ..Default::default() // drain_timeout: graceful in-flight drain on rotation
    };
    let pool = WarmPool::start(fronts, node_b.cert(), transport, cfg).await?;

    println!("pooled multipath proxy:");
    println!("  transport:     oracle-fed CC (Congestion::Quicmix, BDP+tolerant timers)");
    println!("  substrate:     Striped round-robin (2 emulated mixnets per circuit)");
    println!(
        "  pre-warmed:    {} circuits ready, {} distinct sources",
        pool.len().await,
        pool.distinct_sources().await
    );

    let proxy_addr = pool.clone().serve("127.0.0.1:0").await?;
    println!("  proxy:         http://{proxy_addr}  →  gateway → origin {origin_addr}\n");

    // Fire `requests` concurrent fetches through the proxy.
    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..requests {
        handles.push(tokio::spawn(fetch_once(proxy_addr, origin_addr)));
    }
    let mut ok = 0usize;
    for h in handles {
        if matches!(h.await, Ok(Ok(true))) {
            ok += 1;
        }
    }
    let elapsed = start.elapsed();

    println!(
        "{ok}/{requests} concurrent requests OK in {:.0} ms",
        elapsed.as_secs_f64() * 1e3
    );

    // Let the maintainer settle: over-used circuits (max_uses=3) retire and are
    // replaced off the hot path. The pool should heal back to `target`.
    tokio::time::sleep(Duration::from_millis(600)).await;
    println!(
        "after rotation settles: {} circuits ready (target {}), {} built total = {} rotations",
        pool.len().await,
        cfg.target,
        pool.built(),
        pool.built().saturating_sub(cfg.target as u64),
    );
    println!(
        "=> requests round-robined across rotating, pre-warmed circuits over a striped\n   substrate, all under the oracle-fed CC; the pool self-heals + rotates off the hot path."
    );

    // Observability contract (#7): the structured metrics scrape for this run.
    println!("\n--- metrics (prometheus exposition) ---");
    print!("{}", pool.metrics_prometheus().await);

    if ok != requests {
        anyhow::bail!("{}/{} requests failed", requests - ok, requests);
    }
    Ok(())
}
