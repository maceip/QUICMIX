//! quicmix benchmark: transfer a payload over real QUIC (quinn) through the
//! mixnet emulator and compare congestion behaviour.
//!
//! Three arms isolate *why* a mixnet-native client wins:
//!   * `cubic`        — stock QUIC (CUBIC). Reads mix reordering + drops as
//!                      congestion and collapses.
//!   * `cubic+timers` — CUBIC with reorder-tolerant loss detection (oracle-seeded
//!                      `initial_rtt`, high `packet_threshold`). Recovers the
//!                      reordering losses but still backs off on real drops.
//!   * quicmix        — fixed BDP window + loss ignored as congestion + tolerant
//!                      timers. Treats mix drops as ARQ, not congestion.
//!
//! Usage: `cargo run --manifest-path quicmix/Cargo.toml --bin bench -- [MB]`

use anyhow::Result;
use quicmix::client::{bdp_bytes, Congestion};
use quicmix::{emulator::EmulatedMixnet, relay::start_relay, OracleParams};
use quinn::{ClientConfig, Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn make_cert() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
    let c = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert = c.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(c.key_pair.serialize_der()));
    Ok((cert, key))
}

/// Egress buffer size (packets) ≈ one BDP — a classic single-bottleneck buffer.
fn buffer_packets(p: &OracleParams) -> usize {
    ((bdp_bytes(p) / p.mtu as u64) as usize).max(16)
}

struct Sample {
    fct: Duration,
    goodput: f64,
    lost: u64,
    sent: u64,
}

async fn run_once(cc: Congestion, payload_len: usize, p: OracleParams) -> Result<Sample> {
    let (cert, key) = make_cert()?;
    let tc = cc.transport(&p);

    // Server: accept one connection, drain one uni stream, then close.
    let mut server_config = ServerConfig::with_single_cert(vec![cert.clone()], key)?;
    server_config.transport_config(tc.clone());
    let server = Endpoint::server(server_config, "127.0.0.1:0".parse()?)?;
    let server_addr = server.local_addr()?;
    {
        let server = server.clone();
        tokio::spawn(async move {
            if let Some(incoming) = server.accept().await {
                if let Ok(conn) = incoming.await {
                    if let Ok(mut s) = conn.accept_uni().await {
                        let _ = s.read_to_end(payload_len + 4096).await;
                    }
                    conn.close(0u32.into(), b"done");
                }
            }
        });
    }

    // Mix relay (emulator) between client and server — finite egress buffers so
    // overload causes load-dependent tail-drop, not free unbounded queueing.
    let buf = buffer_packets(&p);
    let front_addr = start_relay(
        server_addr,
        std::sync::Arc::new(EmulatedMixnet::with_queue(p, buf)),
        std::sync::Arc::new(EmulatedMixnet::with_queue(p, buf)),
    )
    .await?;

    // Client.
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert)?;
    let mut client_config = ClientConfig::with_root_certificates(Arc::new(roots))?;
    client_config.transport_config(tc);
    let mut client = Endpoint::client("127.0.0.1:0".parse()?)?;
    client.set_default_client_config(client_config);

    let conn = client.connect(front_addr, "localhost")?.await?;

    // Time the data transfer (handshake excluded).
    let payload = vec![0u8; payload_len];
    let start = Instant::now();
    let mut s = conn.open_uni().await?;
    s.write_all(&payload).await?;
    s.finish()?;
    let _ = conn.closed().await; // server closes once it has all bytes
    let fct = start.elapsed();

    let stats = conn.stats();
    let lost = stats.path.lost_packets;
    let sent = stats.path.sent_packets;

    server.close(0u32.into(), b"bye");
    client.close(0u32.into(), b"bye");

    Ok(Sample {
        fct,
        goodput: payload_len as f64 / fct.as_secs_f64(),
        lost,
        sent,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let mb: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let payload = mb * 1024 * 1024;

    // A constant-rate mix: ~1.2 MB/s budget (one ~1200B cell per ms), 3 hops.
    let p = OracleParams {
        hops: 3,
        mean_hop_delay: Duration::from_millis(5),
        drop_prob: 0.02,
        slot_interval: Duration::from_millis(1),
        mtu: 1200,
    };
    let cap = p.mtu as f64 / p.slot_interval.as_secs_f64() / 1e6;

    println!(
        "scenario: {mb} MB | hops={} mean_hop_delay={:?} drop={} rate_cap≈{:.2} MB/s",
        p.hops, p.mean_hop_delay, p.drop_prob, cap
    );
    println!(
        "{:<14} {:>9} {:>14} {:>10} {:>9}",
        "cc", "fct(s)", "goodput(MB/s)", "lost/sent", "loss%"
    );
    let mut baseline = None;
    for cc in [Congestion::Stock, Congestion::Timers, Congestion::Quicmix] {
        let s = run_once(cc, payload, p).await?;
        let loss_pct = if s.sent > 0 {
            100.0 * s.lost as f64 / s.sent as f64
        } else {
            0.0
        };
        let ratio = match baseline {
            Some(b) => format!("  ({:.1}x)", s.goodput / b),
            None => {
                baseline = Some(s.goodput);
                String::new()
            }
        };
        println!(
            "{:<14} {:>9.3} {:>14.2} {:>10} {:>8.1}%{}",
            cc.label(),
            s.fct.as_secs_f64(),
            s.goodput / 1e6,
            format!("{}/{}", s.lost, s.sent),
            loss_pct,
            ratio
        );
    }
    Ok(())
}
