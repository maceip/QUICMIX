//! Nym substrate **sidecar** — exposes a local UDP front that tunnels quicmix's QUIC
//! over the real Nym mixnet, so the lean `quicmix` client can use Nym without linking
//! nym-sdk (and its sqlite) in-process. This is what lets a single client offer both
//! Nym and Tor: they cannot be linked into one binary (conflicting `libsqlite3-sys`),
//! but as separate sidecar processes they coexist freely.
//!
//! Sidecar contract (see `quicmix::front`): print
//!
//! ```text
//! FRONT <ip:port>
//! ```
//!
//! once on stdout, then run until killed. The client dials that front; bytes traverse
//! the Nym mixnet to the gateway.
//!
//! Config:
//!   QUICMIX_NYM_GATEWAY  base58 Nym address of the gateway (required — Nym addresses
//!                        by mixnet identity, not by the IP the directory publishes,
//!                        so this is supplied out-of-band; gateway-side Nym-address
//!                        discovery is the documented follow-up)
//!   QUICMIX_NYM_SURBS    reply-SURB budget per datagram (default 4)

use quicmix_nym::{nym_oracle, spawn_client_bridge, NymSubstrate};
use std::io::Write;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let gateway = std::env::var("QUICMIX_NYM_GATEWAY")
        .map_err(|_| anyhow::anyhow!("set QUICMIX_NYM_GATEWAY=<base58 Nym address>"))?;
    let surbs: u32 = std::env::var("QUICMIX_NYM_SURBS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    eprintln!("nym-sidecar: bootstrapping ephemeral Nym client -> {gateway} …");
    let sub = Arc::new(NymSubstrate::connect(&gateway, surbs, nym_oracle()).await?);
    let (front, _boundary) = spawn_client_bridge(sub).await?;

    // Announce the front (flush: stdout is block-buffered when piped to the client).
    println!("FRONT {front}");
    std::io::stdout().flush().ok();
    eprintln!("nym-sidecar: front ready at {front} -> {gateway}");

    // Keep the bridge + Nym client alive until killed.
    std::future::pending::<()>().await;
    Ok(())
}
