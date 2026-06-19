//! quicmix rotation demo: show that rotating to a pre-warmed, unlinkable circuit
//! is far cheaper than building a fresh one on the hot path, and that the logical
//! session continues across circuits that share nothing observable.
//!
//! Usage: `cargo run --manifest-path quicmix/Cargo.toml --bin rotate`

use anyhow::Result;
use quicmix::rotation::{connect_fresh, session_send, CircuitPool};
use quicmix::OracleParams;
use quinn::{Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

type Sessions = Arc<Mutex<HashMap<[u8; 8], Vec<SocketAddr>>>>;

fn make_cert() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
    let c = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert = c.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(c.key_pair.serialize_der()));
    Ok((cert, key))
}

/// Server: for each connection, read `session_id || msg` off each bidi stream,
/// record which apparent source addresses carried each session, and ack.
fn spawn_server(cert: CertificateDer<'static>, key: PrivateKeyDer<'static>) -> Result<(SocketAddr, Sessions)> {
    let server_config = ServerConfig::with_single_cert(vec![cert], key)?;
    let endpoint = Endpoint::server(server_config, "127.0.0.1:0".parse()?)?;
    let addr = endpoint.local_addr()?;
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
    let sessions2 = sessions.clone();
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let sessions = sessions2.clone();
            tokio::spawn(async move {
                let Ok(conn) = incoming.await else { return };
                let remote = conn.remote_address();
                while let Ok((mut s, mut r)) = conn.accept_bi().await {
                    let Ok(data) = r.read_to_end(1 << 20).await else { break };
                    if data.len() >= 8 {
                        let mut sid = [0u8; 8];
                        sid.copy_from_slice(&data[..8]);
                        sessions.lock().await.entry(sid).or_default().push(remote);
                    }
                    let _ = s.write_all(b"k").await;
                    let _ = s.finish();
                }
            });
        }
    });
    Ok((addr, sessions))
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let (cert, key) = make_cert()?;
    let (server_addr, sessions) = spawn_server(cert.clone(), key)?;

    // A mix with real per-hop latency (no drops; we're measuring rotation cost).
    let p = OracleParams {
        hops: 3,
        mean_hop_delay: Duration::from_millis(10),
        drop_prob: 0.0,
        slot_interval: Duration::ZERO,
        mtu: 1200,
    };
    let one_way = p.mean_hop_delay * p.hops;
    println!(
        "mix: {} hops, ~{:?} one-way (~{:?} RTT)\n",
        p.hops,
        one_way,
        one_way * 2
    );

    // ---- Rotation cost: medians over K samples (a single Erlang RTT is noisy).
    let k = 11usize;
    // COLD: build a brand-new circuit on the hot path each time, then send.
    let mut cold = Vec::new();
    for _ in 0..k {
        let t = Instant::now();
        let c = connect_fresh(server_addr, cert.clone(), p).await?;
        session_send(&c.conn, rand_id(), b"x").await?;
        cold.push(t.elapsed());
    }
    // WARM: pre-warm a pool off the hot path, then time take + send.
    let mut pool = CircuitPool::prewarm(k, server_addr, cert.clone(), p).await?;
    println!("pre-warmed pool: {} circuits ready", pool.len());
    let mut warm = Vec::new();
    while let Some(c) = pool.take() {
        let t = Instant::now();
        session_send(&c.conn, rand_id(), b"x").await?;
        warm.push(t.elapsed());
    }
    let cold_med = median_ms(&mut cold);
    let warm_med = median_ms(&mut warm);

    println!();
    println!("rotation cost (median of {k}, establish-if-needed + first round-trip):");
    println!("  cold (build fresh circuit): {cold_med:>7.1} ms  (handshake + data RTT)");
    println!("  warm (take from pool):      {warm_med:>7.1} ms  (data RTT only)");
    if warm_med > 0.0 {
        println!("  speedup: {:.1}x", cold_med / warm_med);
    }

    // ---- Continuity + unlinkability: same session id over two fresh circuits.
    let sid = rand_id();
    let cold_c = connect_fresh(server_addr, cert.clone(), p).await?;
    session_send(&cold_c.conn, sid, b"a").await?;
    let mut pool2 = CircuitPool::prewarm(1, server_addr, cert.clone(), p).await?;
    let warm_c = pool2.take().unwrap();
    session_send(&warm_c.conn, sid, b"b").await?;

    let map = sessions.lock().await;
    let addrs = map.get(&sid).cloned().unwrap_or_default();
    let distinct: std::collections::HashSet<_> = addrs.iter().collect();
    println!();
    println!("session continuity / unlinkability:");
    println!("  session id (e2e, inside tunnel): {}", hex(&sid));
    println!(
        "  carried over {} circuits; {} distinct apparent source addrs: {:?}",
        addrs.len(),
        distinct.len(),
        addrs
    );
    println!(
        "  circuit-1 local addr: {}   circuit-2 local addr: {}",
        cold_c.local, warm_c.local
    );
    assert_eq!(addrs.len(), 2, "session seen on both circuits");
    assert_eq!(distinct.len(), 2, "the two circuits look unrelated on the wire");
    assert_ne!(cold_c.local, warm_c.local, "fresh endpoint per circuit");
    println!(
        "  => same logical session, no shared source addr / keys / resumption ticket. OK"
    );

    Ok(())
}

fn median_ms(samples: &mut [Duration]) -> f64 {
    samples.sort();
    samples[samples.len() / 2].as_secs_f64() * 1e3
}

fn rand_id() -> [u8; 8] {
    use rand::RngCore;
    let mut b = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut b);
    b
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
