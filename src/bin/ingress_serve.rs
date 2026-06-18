//! quicmix ingress proxy — a local HTTP proxy that tunnels over QUIC to a remote
//! gateway. Point apps (curl -x, browser) at it; traffic egresses at the gateway,
//! under quicmix's oracle-fed congestion control.
//!
//! run: `ingress_serve <gateway_addr> <gateway_cert_file> <proxy_listen> [bind]`
//!   e.g. `ingress_serve 1.2.3.4:4433 /tmp/quicmix-gw.cert 127.0.0.1:8888`

use anyhow::Result;
use quicmix::node::Node;
use quicmix::OracleParams;
use rustls::pki_types::CertificateDer;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider().install_default().ok();
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 4 {
        eprintln!("usage: ingress_serve <gateway_addr> <gateway_cert_file> <proxy_listen> [bind]");
        std::process::exit(2);
    }
    let gateway: std::net::SocketAddr = a[1].parse()?;
    let cert = CertificateDer::from(std::fs::read(&a[2])?);
    let proxy_listen = a[3].clone();
    let bind: std::net::SocketAddr = a.get(4).map(|s| s.as_str()).unwrap_or("0.0.0.0:0").parse()?;

    let p = OracleParams {
        hops: 1,
        mean_hop_delay: Duration::from_millis(30),
        drop_prob: 0.0,
        slot_interval: Duration::ZERO,
        mtu: 1200,
    };
    let node = Node::new(p)?;
    eprintln!("connecting to gateway {gateway} (oracle-fed CC) …");
    let link = node.connect_via(bind, gateway, cert).await?;
    // Best-in-class hyper-based HTTP(S) proxy (CONNECT tunnels + forward proxying).
    let proxy = quicmix::ingress::serve(&proxy_listen, link.conn.clone()).await?;
    let _keep = link.endpoint;
    println!("quicmix ingress proxy on http://{proxy}  ->  gateway {gateway}");
    println!("test:  curl -x http://{proxy} http://checkip.amazonaws.com");
    println!("ctrl-c to stop.");
    tokio::signal::ctrl_c().await.ok();
    Ok(())
}
