//! nym_bench — **congestion-control A/B over the real Nym mainnet** (T3).
//!
//! The emulator bench (`quicmix/src/bin/bench.rs`) compared `cubic` vs
//! `cubic+timers` vs `quicmix` over `EmulatedMixnet`. This runs ONE arm over the
//! **live Nym mixnet** (pick with `QUICMIX_CC=stock|timers|quicmix`), uploading a
//! payload over a real quinn uni-stream that traverses Nym, and reports flow
//! completion time, goodput, and retransmits. Run it once per arm and compare.
//!
//! Upload direction is deliberate: the bulk flows client→server, so the return
//! path (over reply-SURBs) carries only QUIC ACKs — keeping SURB pressure low so
//! the measurement reflects CC, not SURB exhaustion.
//!
//!   `QUICMIX_CC=stock   cargo run --release -p quicmix-nym --bin nym_bench`
//!   `QUICMIX_CC=quicmix cargo run --release -p quicmix-nym --bin nym_bench`
//!   (optional `QUICMIX_KB=16` payload size)

use anyhow::Result;
use quicmix::client::Congestion;
use quicmix::OracleParams;
use quicmix_nym::{spawn_client_bridge, NymGateway, NymSubstrate};
use quinn::{ClientConfig, Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider().install_default().ok();

    let cc = match std::env::var("QUICMIX_CC").as_deref() {
        Ok("stock") => Congestion::Stock,
        Ok("timers") => Congestion::Timers,
        _ => Congestion::Quicmix,
    };
    let kb: usize = std::env::var("QUICMIX_KB").ok().and_then(|s| s.parse().ok()).unwrap_or(16);
    let payload_len = kb * 1024;
    let p = nym_oracle();
    let tc = cc.transport(&p);
    eprintln!("arm = {} | payload = {kb} KB | over REAL Nym (rtt≈{:?})", cc.label(), p.rtt());

    // ---- Server (behind the Nym gateway): drain one uni stream, then close.
    let (cert, key) = make_cert()?;
    let mut server_config = ServerConfig::with_single_cert(vec![cert.clone()], key)?;
    server_config.transport_config(tc.clone());
    let server = Endpoint::server(server_config, "127.0.0.1:0".parse()?)?;
    let server_udp = server.local_addr()?;
    {
        let server = server.clone();
        let want = payload_len;
        tokio::spawn(async move {
            if let Some(incoming) = server.accept().await {
                if let Ok(conn) = incoming.await {
                    if let Ok(mut s) = conn.accept_uni().await {
                        let _ = s.read_to_end(want + 4096).await;
                    }
                    conn.close(0u32.into(), b"done");
                }
            }
        });
    }

    // ---- Nym gateway bridging into the server.
    eprintln!("bootstrapping gateway Nym client…");
    let gw = NymGateway::connect().await?;
    let gw_addr = gw.nym_address().to_string();
    tokio::spawn(async move {
        if let Err(e) = gw.run(server_udp).await {
            eprintln!("gateway loop ended: {e:#}");
        }
    });

    // ---- Client substrate + UDP bridge.
    eprintln!("bootstrapping client Nym substrate…");
    let surbs: u32 = std::env::var("NYM_SURBS").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    let sub = Arc::new(NymSubstrate::connect(&gw_addr, surbs, p).await?);
    let (front, bridge) = spawn_client_bridge(sub).await?;

    // ---- Client quinn endpoint with this arm's transport.
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert)?;
    let mut client_config = ClientConfig::with_root_certificates(Arc::new(roots))?;
    client_config.transport_config(tc);
    let mut client = Endpoint::client("127.0.0.1:0".parse()?)?;
    client.set_default_client_config(client_config);

    eprintln!("QUIC handshake over Nym…");
    let conn = client.connect(front, "localhost")?.await?;

    // ---- Timed upload (handshake excluded).
    let payload = vec![0u8; payload_len];
    let start = Instant::now();
    let mut s = conn.open_uni().await?;
    s.write_all(&payload).await?;
    s.finish()?;
    let _ = conn.closed().await;
    let fct = start.elapsed();

    let stats = conn.stats();
    let lost = stats.path.lost_packets;
    let sent = stats.path.sent_packets;
    let loss_pct = if sent > 0 { 100.0 * lost as f64 / sent as f64 } else { 0.0 };
    let goodput = payload_len as f64 / fct.as_secs_f64();

    println!(
        "\n{:<14} fct={:.1}s  goodput={:.2} KB/s  lost/sent={}/{}  loss={:.1}%",
        cc.label(),
        fct.as_secs_f64(),
        goodput / 1024.0,
        lost,
        sent,
        loss_pct
    );
    let bm = bridge.metrics();
    println!(
        "  substrate boundary: sent={} recv={} send_errors={} dropped={} queue_depth={}",
        bm.sent, bm.received, bm.send_errors, bm.dropped, bm.queue_depth
    );
    Ok(())
}
