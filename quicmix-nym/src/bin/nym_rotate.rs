//! nym_rotate — **unlinkable circuit rotation over the real Nym mainnet** (T4).
//!
//! The emulator rotation demo (`quicmix/src/bin/rotate.rs`) showed warm-vs-cold
//! and unlinkability over `EmulatedMixnet`. This runs the *same* rotation logic —
//! now substrate-agnostic via [`quicmix::rotation::connect_fresh_with`] +
//! [`quicmix_nym::nym_front`] — over **real Nym circuits**. A rotated circuit is a
//! brand-new ephemeral Nym client (new mixnet identity + SURB context) behind a
//! fresh QUIC connection (no resumption ticket): unlinkable at both layers.
//!
//! On a real mixnet the case for a pre-warmed pool is far stronger than on the
//! emulator: a cold rotation pays the *entire Nym client bootstrap* (tens of
//! seconds) on the hot path; a warm one pays only a data round-trip.
//!
//! Run (open egress; slow — bootstraps several Nym clients):
//!   `cargo run --release -p quicmix-nym --bin nym_rotate`

use anyhow::Result;
use quicmix::client::Congestion;
use quicmix::rotation::{connect_fresh_with, session_send, CircuitPool};
use quicmix::OracleParams;
use quicmix_nym::{nym_front, NymGateway};
use quinn::{Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

type Sessions = Arc<Mutex<HashMap<[u8; 8], Vec<SocketAddr>>>>;

fn nym_oracle() -> OracleParams {
    OracleParams {
        hops: 3,
        mean_hop_delay: Duration::from_millis(470),
        drop_prob: 0.0,
        slot_interval: Duration::from_millis(158),
        mtu: 1200,
    }
}

fn make_cert() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
    let c = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert = c.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(c.key_pair.serialize_der()));
    Ok((cert, key))
}

/// Session-recording quinn server (behind the Nym gateway bridge): for each bidi
/// stream it reads `session_id || msg`, records the apparent source address that
/// carried each session, and acks. Distinct sources for one session id ⇒ the
/// circuits look unrelated on the wire.
fn spawn_server(
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
    p: OracleParams,
) -> Result<(SocketAddr, Sessions)> {
    let mut server_config = ServerConfig::with_single_cert(vec![cert], key)?;
    // The server end must also tolerate seconds-scale mixnet RTT (long idle
    // timeout, seeded initial_rtt) — same oracle-tuned transport as the client.
    server_config.transport_config(Congestion::Quicmix.transport(&p));
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

fn rand_id() -> [u8; 8] {
    // The session id only needs to be *distinct* within this run (it lives inside
    // the encrypted tunnel). A mixed monotonic counter guarantees that without an
    // RNG dep.
    use std::sync::atomic::{AtomicU64, Ordering};
    static CTR: AtomicU64 = AtomicU64::new(0x1234_5678_9abc_def0);
    let mut x = CTR.fetch_add(0x9E3779B97F4A7C15, Ordering::Relaxed);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58476D1CE4E5B9);
    x ^= x >> 27;
    x.to_le_bytes()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider().install_default().ok();
    let p = nym_oracle();

    // ---- Server reached through the Nym gateway bridge.
    let (cert, key) = make_cert()?;
    let (server_udp, sessions) = spawn_server(cert.clone(), key, p)?;
    // Oracle-tuned transport for the rotated client circuits (see connect_fresh_with).
    let transport = Some(Congestion::Quicmix.transport(&p));
    eprintln!("bootstrapping gateway Nym client to mainnet…");
    let gw = NymGateway::connect().await?;
    let gw_addr = gw.nym_address().to_string();
    eprintln!("gateway Nym address: {gw_addr}");
    tokio::spawn(async move {
        if let Err(e) = gw.run(server_udp).await {
            eprintln!("gateway loop ended: {e:#}");
        }
    });

    // Front factory: each call = a fresh ephemeral Nym circuit to the gateway.
    let front = nym_front(gw_addr, 8, p);

    // Sample counts — Nym RTT is high-variance (p50≈2.8 s, p90≈4.6 s), so single
    // samples are noisy; take medians. Cold samples each bootstrap a fresh Nym
    // client, so keep them few.
    let n_cold: usize = std::env::var("NYM_COLD").ok().and_then(|s| s.parse().ok()).unwrap_or(2);
    let n_warm: usize = std::env::var("NYM_WARM").ok().and_then(|s| s.parse().ok()).unwrap_or(4);

    // ---- COLD: each sample builds a brand-new Nym circuit on the hot path, sends.
    eprintln!("\nCOLD rotation x{n_cold}: fresh Nym client bootstrap + handshake on the hot path…");
    let mut cold_samples = Vec::new();
    let mut cold_c = None;
    for _ in 0..n_cold.max(1) {
        let t = Instant::now();
        let c = connect_fresh_with(&front, cert.clone(), transport.clone()).await?;
        session_send(&c.conn, rand_id(), b"x").await?;
        cold_samples.push(t.elapsed());
        cold_c = Some(c); // keep the last cold circuit for the continuity check
    }
    let cold_c = cold_c.unwrap();

    // ---- WARM: pre-warm a pool off the hot path, then time take + send per sample.
    let k = n_warm + 1; // +1 spare for the continuity check
    eprintln!("pre-warming {k} Nym circuits off the hot path (each a new Nym identity)…");
    let mut pool = CircuitPool::prewarm_with(k, &front, cert.clone(), transport.clone()).await?;
    eprintln!("pre-warmed pool: {} circuits ready", pool.len());
    let mut warm_samples = Vec::new();
    for _ in 0..n_warm {
        if let Some(c) = pool.take() {
            let t = Instant::now();
            session_send(&c.conn, rand_id(), b"x").await?;
            warm_samples.push(t.elapsed());
        }
    }
    let median = |v: &mut Vec<Duration>| -> f64 {
        v.sort();
        if v.is_empty() { 0.0 } else { v[v.len() / 2].as_secs_f64() }
    };
    let cold_med = median(&mut cold_samples);
    let warm_med = median(&mut warm_samples);

    println!("\nrotation cost over REAL Nym (medians; establish-if-needed + first round-trip):");
    println!("  cold (fresh Nym client + handshake): {:>7.1} s  (n={n_cold})", cold_med);
    println!("  warm (take from pre-warmed pool):    {:>7.1} s  (data round-trip only, n={})", warm_med, warm_samples.len());
    if warm_med > 0.0 {
        println!("  speedup: {:.1}x  (warm pre-pays the whole Nym client bootstrap)", cold_med / warm_med);
    }

    // ---- Continuity + unlinkability: same session id over two distinct circuits.
    let sid = rand_id();
    session_send(&cold_c.conn, sid, b"a").await?;
    let warm_c = pool.take();
    let warm_c = match warm_c {
        Some(c) => c,
        None => connect_fresh_with(&front, cert.clone(), transport.clone()).await?,
    };
    session_send(&warm_c.conn, sid, b"b").await?;

    // give the server a moment to record both
    tokio::time::sleep(Duration::from_secs(2)).await;
    let map = sessions.lock().await;
    let addrs = map.get(&sid).cloned().unwrap_or_default();
    let distinct: std::collections::HashSet<_> = addrs.iter().collect();
    println!("\nsession continuity / unlinkability over real Nym:");
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
    if addrs.len() == 2 && distinct.len() == 2 && cold_c.local != warm_c.local {
        println!("  => same logical session over two fresh Nym identities, no shared source/keys/ticket. OK");
    } else {
        println!("  => (note) expected 2 distinct sources; got {addrs:?} — see gateway demux");
    }
    Ok(())
}
