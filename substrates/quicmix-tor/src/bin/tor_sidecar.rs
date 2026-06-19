//! Tor substrate **sidecar** — exposes a local UDP front that tunnels quicmix's QUIC
//! over a real Tor circuit, so the lean `quicmix` client can use Tor without linking
//! arti (and its sqlite) in-process.
//!
//! Sidecar contract (see `quicmix::front`): bootstrap the substrate, then print
//!
//! ```text
//! FRONT <ip:port>
//! ```
//!
//! once on stdout and run until killed. The client dials that front; bytes traverse
//! Tor to the gateway.
//!
//! Config:
//!   QUICMIX_SIDECAR_TARGET  gateway `host:port` (set by the client when it spawns us)
//!   QUICMIX_TOR_GATEWAY     same, for running the sidecar standalone
//!
//! Tor's exit connects to the gateway's public `host:port` directly, so this needs no
//! gateway-side changes — the same quinn gateway the autopilot already runs is the
//! target. Live bootstrap needs open egress to the Tor network (run on a laptop).

use quicmix::front::{spawn_substrate_front, SIDECAR_TARGET_ENV};
use quicmix::OracleParams;
use quicmix_tor::TorSubstrate;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

/// Tor is a single ordered stream, not a rate-capped mixnet: `slot_interval` is ZERO
/// (uncapped link), with a representative 3-relay delay. quicmix's CC does not govern
/// Tor's internal transport; this only feeds RTT/loss-timer derivation.
fn tor_oracle() -> OracleParams {
    OracleParams {
        hops: 3,
        mean_hop_delay: Duration::from_millis(100),
        drop_prob: 0.0,
        slot_interval: Duration::ZERO,
        mtu: 1200,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let target = std::env::var(SIDECAR_TARGET_ENV)
        .or_else(|_| std::env::var("QUICMIX_TOR_GATEWAY"))
        .map_err(|_| {
            anyhow::anyhow!("set {SIDECAR_TARGET_ENV} or QUICMIX_TOR_GATEWAY=<host:port>")
        })?;

    eprintln!("tor-sidecar: bootstrapping Tor + opening a circuit to {target} …");
    let sub = Arc::new(TorSubstrate::connect(&target, tor_oracle()).await?);
    let (front, _boundary) = spawn_substrate_front(sub).await?;

    // Announce the front (flush: stdout is block-buffered when piped to the client).
    println!("FRONT {front}");
    std::io::stdout().flush().ok();
    eprintln!("tor-sidecar: front ready at {front} -> {target}");

    // Keep the bridge + Tor circuit alive until killed.
    std::future::pending::<()>().await;
    Ok(())
}
