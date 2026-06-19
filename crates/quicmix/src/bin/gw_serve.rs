//! quicmix gateway server — a public QUIC gateway that egresses to the internet.
//! Writes its pinned self-signed cert to a file so a remote ingress can trust it.
//!
//! run: `gw_serve [bind_addr] [cert_out_file]`
//!   e.g. `gw_serve 0.0.0.0:4433 /tmp/quicmix-gw.cert`
//!
//! Targets requested by a connected ingress (CONNECT / absolute-URI) are dialed
//! from THIS host — so traffic appears to originate here.

use anyhow::Result;
use quicmix::node::Node;
use quicmix::OracleParams;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider().install_default().ok();
    let a: Vec<String> = std::env::args().collect();
    let bind = a.get(1).cloned().unwrap_or_else(|| "0.0.0.0:4433".into());
    let cert_out = a.get(2).cloned().unwrap_or_else(|| "quicmix-gw.cert".into());

    // Direct-internet path model: no rate cap, ~typical WAN per-hop delay.
    let p = OracleParams {
        hops: 1,
        mean_hop_delay: Duration::from_millis(30),
        drop_prob: 0.0,
        slot_interval: Duration::ZERO,
        mtu: 1200,
    };
    let node = Node::new(p)?;
    let (ep, addr) = node.serve_gateway_at(bind.parse()?).await?;
    std::fs::write(&cert_out, node.cert().as_ref())?;
    println!("quicmix gateway up on {addr}");
    println!("cert -> {cert_out} ({} bytes)", node.cert().as_ref().len());
    println!("egresses CONNECT / absolute-uri targets from this host. ctrl-c to stop.");
    let _keep = ep;
    tokio::signal::ctrl_c().await.ok();
    Ok(())
}
