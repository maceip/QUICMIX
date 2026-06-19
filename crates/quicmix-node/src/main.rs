//! quicmix — **one binary, no role flag.**
//!
//! The node discovers the IP it egresses on; if that IP is public it
//! **auto-promotes to a gateway** (serves QUIC, publishes its cert, announces
//! itself), otherwise it runs as **ingress**. Either way it joins the gateway
//! directory and serves traffic. Role is environment, not a subcommand.
//!
//! This is the **composition binary**: it links the sqlite-free substrate crates
//! (Katzenpost, HOPR) behind features and injects a
//! [`quicmix::front::SubstrateBuilder`], so those substrates are selectable
//! **in-process**. Nym and Tor are reached via sidecar processes (they cannot be
//! linked together — conflicting `libsqlite3-sys`).
//!
//! Config (all optional, with defaults):
//!   QUICMIX_GW_BIND      gateway QUIC + cert bind   (default 0.0.0.0:4433)
//!   QUICMIX_INGRESS_BIND local proxy bind           (default 127.0.0.1:8888)
//!   QUICMIX_BOOTSTRAP    comma-sep ip:port peers    (default: built-in bootstrap nodes)
//!   QUICMIX_PUBLIC_IP    force-promote with this ip (cloud NAT escape hatch)
//!   QUICMIX_CERT_OUT     also persist the gateway cert to this path
//!
//! Substrate selection (ingress leg):
//!   QUICMIX_SUBSTRATE    direct (default) | katzenpost | hopr   (linked, this binary)
//!                        | nym | tor                            (sidecar)
//!   linked substrate config:
//!     katzenpost — QUICMIX_KP_DAEMON=host:port, QUICMIX_KP_CAPABILITY (default quicmix)
//!     hopr       — QUICMIX_HOPR_API, QUICMIX_HOPR_TOKEN, QUICMIX_HOPR_DESTINATION,
//!                  QUICMIX_HOPR_HOPS (default 1)
//!   sidecar substrate config:
//!     QUICMIX_SIDECAR_FRONT=ip:port  (attach to a running sidecar), or
//!     QUICMIX_SIDECAR_CMD="<prog> <args>"  (spawn one; e.g. tor_sidecar / nym_sidecar)
//!
//! run: `quicmix`   (then `curl -x http://127.0.0.1:8888 http://checkip.amazonaws.com`)

use anyhow::Result;
use quicmix::autopilot::{self, Config};
use quicmix::front::SubstrateBuilder;
use std::sync::Arc;
#[cfg(any(feature = "katzenpost", feature = "hopr"))]
use quicmix::MixTransport;
#[cfg(any(feature = "katzenpost", feature = "hopr"))]
use std::net::SocketAddr;

/// Constructs the substrates **linked into this binary** (the sqlite-free ones). Each
/// `--features` arm adds its constructor; with no features it is not compiled at all and
/// the node offers only `direct` + sidecar substrates.
#[cfg(any(feature = "katzenpost", feature = "hopr"))]
struct LinkedSubstrates;

#[cfg(any(feature = "katzenpost", feature = "hopr"))]
#[async_trait::async_trait]
impl SubstrateBuilder for LinkedSubstrates {
    async fn build(&self, name: &str, gateway: SocketAddr) -> Result<Arc<dyn MixTransport>> {
        // `gateway` is used by IP-addressed substrates (HOPR); silence when that arm
        // isn't compiled in.
        #[cfg(not(feature = "hopr"))]
        let _ = gateway;
        match name.to_ascii_lowercase().as_str() {
            #[cfg(feature = "katzenpost")]
            "katzenpost" | "kp" => build_katzenpost().await,
            #[cfg(feature = "hopr")]
            "hopr" => build_hopr(gateway).await,
            other => anyhow::bail!(
                "substrate {other:?} is not linked into this binary (linked: {:?}); \
                 for nym/tor run a sidecar (QUICMIX_SIDECAR_CMD / QUICMIX_SIDECAR_FRONT)",
                self.supported()
            ),
        }
    }

    fn supported(&self) -> Vec<&'static str> {
        #[allow(unused_mut)]
        let mut v: Vec<&'static str> = Vec::new();
        #[cfg(feature = "katzenpost")]
        v.push("katzenpost");
        #[cfg(feature = "hopr")]
        v.push("hopr");
        v
    }
}

#[cfg(feature = "katzenpost")]
async fn build_katzenpost() -> Result<Arc<dyn MixTransport>> {
    use quicmix_katzenpost::{katzenpost_oracle, KatzenpostSubstrate};
    let daemon = std::env::var("QUICMIX_KP_DAEMON").map_err(|_| {
        anyhow::anyhow!("katzenpost: set QUICMIX_KP_DAEMON=host:port (thin-client daemon)")
    })?;
    let capability = std::env::var("QUICMIX_KP_CAPABILITY").unwrap_or_else(|_| "quicmix".into());
    let (sub, _hs) =
        KatzenpostSubstrate::connect_service(&daemon, &capability, katzenpost_oracle()).await?;
    Ok(Arc::new(sub))
}

#[cfg(feature = "hopr")]
async fn build_hopr(gateway: SocketAddr) -> Result<Arc<dyn MixTransport>> {
    use quicmix_hopr::{hopr_oracle, HoprSubstrate, SessionOpts};
    let api = std::env::var("QUICMIX_HOPR_API")
        .map_err(|_| anyhow::anyhow!("hopr: set QUICMIX_HOPR_API=http://host:port"))?;
    let token =
        std::env::var("QUICMIX_HOPR_TOKEN").map_err(|_| anyhow::anyhow!("hopr: set QUICMIX_HOPR_TOKEN"))?;
    let destination = std::env::var("QUICMIX_HOPR_DESTINATION")
        .map_err(|_| anyhow::anyhow!("hopr: set QUICMIX_HOPR_DESTINATION=<on-chain address>"))?;
    let hops: u8 = std::env::var("QUICMIX_HOPR_HOPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    // The HOPR exit node tunnels to the gateway's public UDP addr.
    let opts = SessionOpts {
        hops,
        target: gateway.to_string(),
        ..Default::default()
    };
    let sub = HoprSubstrate::connect(api, token, destination, opts, hopr_oracle(hops)).await?;
    Ok(Arc::new(sub))
}

/// The injected builder, or `None` when no linked substrates were compiled in.
fn substrate_builder() -> Option<Arc<dyn SubstrateBuilder>> {
    #[cfg(any(feature = "katzenpost", feature = "hopr"))]
    {
        Some(Arc::new(LinkedSubstrates))
    }
    #[cfg(not(any(feature = "katzenpost", feature = "hopr")))]
    {
        None
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::from_env()?;
    autopilot::run(cfg, substrate_builder()).await
}
