//! Round-robin multipath demo — stripe one transfer across multiple substrates
//! and show which is faster and why.
//!
//! Runs the same QUIC transfer over: a **fast** substrate alone, a **slow** one
//! alone, and a **round-robin `Striped`** of both. Each emulated substrate stands
//! in for a real one (e.g. a low-latency WireGuard-ish path vs a 5-hop mixnet
//! path). It exposes the honest finding: naive round-robin is bounded by the slow
//! path's head-of-line blocking — motivating a rate-weighted scheduler.
//!
//! Run: `cargo run --manifest-path quicmix/Cargo.toml --bin multipath`

use anyhow::Result;
use quicmix::client::Congestion;
use quicmix::emulator::EmulatedMixnet;
use quicmix::relay::start_relay;
use quicmix::striped::Striped;
use quicmix::{MixTransport, OracleParams};
use quinn::{ClientConfig, Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn prof(slot_us: u64, hops: u32, delay_ms: u64) -> OracleParams {
    OracleParams {
        hops,
        mean_hop_delay: Duration::from_millis(delay_ms),
        drop_prob: 0.01,
        slot_interval: Duration::from_micros(slot_us),
        mtu: 1200,
    }
}

// fast: ~2.4 MB/s, low latency (2 hops × 3 ms). slow: ~0.6 MB/s, 5 hops × 15 ms.
fn fast() -> Arc<dyn MixTransport> {
    Arc::new(EmulatedMixnet::with_queue(prof(500, 2, 3), 128))
}
fn slow() -> Arc<dyn MixTransport> {
    Arc::new(EmulatedMixnet::with_queue(prof(2000, 5, 15), 128))
}

fn make_cert() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
    let c = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    Ok((
        c.cert.der().clone(),
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(c.key_pair.serialize_der())),
    ))
}

async fn run(up: Arc<dyn MixTransport>, down: Arc<dyn MixTransport>, payload_len: usize) -> Result<f64> {
    let (cert, key) = make_cert()?;
    let tc = Congestion::Quicmix.transport(&up.oracle());

    let mut sc = ServerConfig::with_single_cert(vec![cert.clone()], key)?;
    sc.transport_config(tc.clone());
    let server = Endpoint::server(sc, "127.0.0.1:0".parse()?)?;
    let server_addr = server.local_addr()?;
    {
        let server = server.clone();
        tokio::spawn(async move {
            if let Some(inc) = server.accept().await {
                if let Ok(conn) = inc.await {
                    if let Ok(mut s) = conn.accept_uni().await {
                        let _ = s.read_to_end(payload_len + 4096).await;
                    }
                    conn.close(0u32.into(), b"done");
                }
            }
        });
    }

    let front = start_relay(server_addr, up, down).await?.front;

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert)?;
    let mut cc = ClientConfig::with_root_certificates(Arc::new(roots))?;
    cc.transport_config(tc);
    let mut client = Endpoint::client("127.0.0.1:0".parse()?)?;
    client.set_default_client_config(cc);
    let conn = client.connect(front, "localhost")?.await?;

    let payload = vec![0u8; payload_len];
    let start = Instant::now();
    let mut s = conn.open_uni().await?;
    s.write_all(&payload).await?;
    s.finish()?;
    let _ = conn.closed().await;
    let fct = start.elapsed();

    server.close(0u32.into(), b"bye");
    client.close(0u32.into(), b"bye");
    Ok(payload_len as f64 / fct.as_secs_f64())
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let mb: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(2);
    let payload = mb * 1024 * 1024;

    println!("scenario: {mb} MB | fast≈2.4 MB/s (2×3ms)  slow≈0.6 MB/s (5×15ms)\n");

    let g_fast = run(fast(), fast(), payload).await?;
    let g_slow = run(slow(), slow(), payload).await?;
    let g_combo = run(
        Arc::new(Striped::new(vec![fast(), slow()])),
        Arc::new(Striped::new(vec![fast(), slow()])),
        payload,
    )
    .await?;

    println!("{:<22} {:>10}", "path", "goodput(MB/s)");
    println!("{:<22} {:>10.2}", "fast substrate alone", g_fast / 1e6);
    println!("{:<22} {:>10.2}", "slow substrate alone", g_slow / 1e6);
    println!("{:<22} {:>10.2}", "round-robin (both)", g_combo / 1e6);
    println!(
        "\nwhy: naive round-robin sends equal shares, so the slow path's \
         head-of-line blocking bounds the tail. Rate-weighted scheduling \
         (send ∝ each substrate's BDP) is the fix."
    );
    Ok(())
}
