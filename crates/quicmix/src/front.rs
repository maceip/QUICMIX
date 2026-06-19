//! Substrate selection + the client-side UDP **front** the QUIC client dials.
//!
//! The client carries its QUIC over exactly one local UDP front, regardless of which
//! mixnet substrate is in use:
//!
//! - `direct` — no mixnet; the front *is* the gateway's public `ip:port` (one real
//!   internet hop — the live default, unchanged).
//! - any real mixnet (`nym`, `tor`, `katzenpost`, `hopr`) — a **sidecar** process
//!   owns the substrate (and its conflicting native deps, e.g. `libsqlite3-sys` for
//!   Nym vs. arti) and exposes a local UDP front. The client spawns/locates it and
//!   dials that addr. Nym and Tor cannot be linked into the same binary (their sqlite
//!   versions conflict at the `links="sqlite3"` level), so the sidecar boundary is
//!   what lets the client offer *both* without a single-binary link conflict.
//!
//! This keeps the client lean (no nym/arti/sqlite linked in) and uniform (one dial
//! path). The sidecar contract is intentionally tiny: a sidecar prints one line
//!
//! ```text
//! FRONT <ip:port>
//! ```
//!
//! to stdout once its front is ready, then runs until killed. The client passes the
//! gateway it wants reached via the `QUICMIX_SIDECAR_TARGET` env var.
//!
//! ## Linked (in-process) substrates
//!
//! Substrates whose native deps do *not* conflict (Katzenpost's thin-client, HOPR's
//! HTTP/UDP session) can instead be **linked into the client** and selected without a
//! second process. Because those substrate crates depend on `quicmix` (for
//! [`MixTransport`]), `quicmix` cannot depend back on them (Cargo forbids dependency
//! cycles). The composition binary crate links them and injects a [`SubstrateBuilder`]
//! that constructs the chosen substrate; [`resolve`] then bridges it to a local UDP
//! front via [`spawn_substrate_front`]. The lib stays substrate-agnostic; the seam is
//! the [`SubstrateBuilder`] trait object passed in [`resolve`].

use anyhow::{anyhow, Context, Result};
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UdpSocket;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::substrate::Substrate;
use crate::MixTransport;

/// The sidecar handshake line prefix printed on stdout once the front is ready.
const FRONT_LINE: &str = "FRONT ";

/// Env var a spawned sidecar reads to learn the gateway it should reach.
pub const SIDECAR_TARGET_ENV: &str = "QUICMIX_SIDECAR_TARGET";

/// Constructs a linked, in-process substrate transport reaching a gateway. Implemented
/// by the composition binary crate (which links the concrete substrate crates) and
/// injected into [`resolve`] so the substrate-agnostic lib can build a selected
/// substrate without depending on it. See the module docs.
#[async_trait::async_trait]
pub trait SubstrateBuilder: Send + Sync {
    /// Build a fresh substrate [`MixTransport`] for substrate `name`, reaching
    /// `gateway` (used as the session target by substrates that address by IP, e.g.
    /// HOPR; ignored by those that address by mixnet identity, e.g. Katzenpost).
    async fn build(&self, name: &str, gateway: SocketAddr) -> Result<Arc<dyn MixTransport>>;

    /// Substrate names this builder can construct (for diagnostics).
    fn supported(&self) -> Vec<&'static str>;
}

/// How the client obtains its local UDP front for a circuit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubstrateChoice {
    /// No mixnet: dial the gateway's public UDP addr directly (one real hop).
    Direct,
    /// A sidecar is already running and serving a front at this fixed addr.
    SidecarAt(SocketAddr),
    /// Spawn this command per node; it prints `FRONT <ip:port>` and owns the substrate.
    SidecarCmd {
        name: String,
        program: String,
        args: Vec<String>,
    },
    /// A linked, in-process substrate selected by name; built by the injected
    /// [`SubstrateBuilder`] and bridged to a local UDP front.
    Named { name: String },
}

impl SubstrateChoice {
    /// Resolve from the environment:
    /// - `QUICMIX_SUBSTRATE` selects the substrate (default `direct`).
    /// - If a sidecar is configured explicitly (`QUICMIX_SIDECAR_FRONT=ip:port` to
    ///   attach, or `QUICMIX_SIDECAR_CMD="<prog> <args>"` to spawn), that wins.
    /// - Otherwise a non-direct name is a **linked** substrate ([`Self::Named`]),
    ///   resolved by the injected [`SubstrateBuilder`] at [`resolve`] time.
    pub fn from_env() -> Result<Self> {
        let name = std::env::var("QUICMIX_SUBSTRATE").unwrap_or_default();
        let name = name.trim();
        if name.is_empty() || name.eq_ignore_ascii_case("direct") {
            return Ok(Self::Direct);
        }
        if let Ok(front) = std::env::var("QUICMIX_SIDECAR_FRONT") {
            let addr = front
                .trim()
                .parse()
                .map_err(|e| anyhow!("QUICMIX_SIDECAR_FRONT: {e}"))?;
            return Ok(Self::SidecarAt(addr));
        }
        if let Ok(cmd) = std::env::var("QUICMIX_SIDECAR_CMD") {
            let mut parts = cmd.split_whitespace().map(|s| s.to_string());
            let program = parts
                .next()
                .ok_or_else(|| anyhow!("QUICMIX_SIDECAR_CMD is empty"))?;
            let args: Vec<String> = parts.collect();
            return Ok(Self::SidecarCmd {
                name: name.to_string(),
                program,
                args,
            });
        }
        Ok(Self::Named { name: name.to_string() })
    }

    /// Human-readable label for logs / UX.
    pub fn label(&self) -> String {
        match self {
            Self::Direct => "direct quicmix QUIC (1 real hop)".into(),
            Self::SidecarAt(a) => format!("sidecar front @ {a}"),
            Self::SidecarCmd { name, .. } => format!("sidecar:{name}"),
            Self::Named { name } => format!("linked:{name}"),
        }
    }
}

/// A resolved front the QUIC client dials, plus the guard that must stay alive to keep
/// it open (a spawned sidecar child process, killed on drop).
pub struct Front {
    /// The UDP address the quinn client should dial for this circuit.
    pub addr: SocketAddr,
    #[allow(dead_code)] // held purely to keep the sidecar process alive (kill-on-drop).
    keep: FrontGuard,
}

#[allow(dead_code)] // variants are held only to keep the front alive (Drop / pumps).
enum FrontGuard {
    None,
    Child(Child),
    /// A linked substrate's boundary; the spawned bridge pumps hold clones, but we keep
    /// it explicitly so the front is torn down deterministically with the circuit.
    Bridge(Arc<Substrate>),
}

/// Resolve the client's UDP front for one circuit to `gateway` (the gateway's public
/// `ip:port`). For `direct` the front *is* `gateway`; for a sidecar the gateway is
/// passed through as the sidecar's target and the front is the sidecar's local socket;
/// for a [`SubstrateChoice::Named`] linked substrate, `builder` constructs it and it is
/// bridged to a fresh local UDP front.
pub async fn resolve(
    choice: &SubstrateChoice,
    gateway: SocketAddr,
    builder: Option<&dyn SubstrateBuilder>,
) -> Result<Front> {
    match choice {
        SubstrateChoice::Direct => Ok(Front {
            addr: gateway,
            keep: FrontGuard::None,
        }),
        SubstrateChoice::SidecarAt(addr) => Ok(Front {
            addr: *addr,
            keep: FrontGuard::None,
        }),
        SubstrateChoice::SidecarCmd { program, args, .. } => {
            spawn_sidecar(program, args, gateway).await
        }
        SubstrateChoice::Named { name } => {
            let builder = builder.ok_or_else(|| {
                anyhow!(
                    "substrate {name:?} is not linked into this binary and no sidecar is \
                     configured; build the composition crate with --features {name} (linked) \
                     or set QUICMIX_SIDECAR_FRONT / QUICMIX_SIDECAR_CMD (sidecar)"
                )
            })?;
            let transport = builder.build(name, gateway).await.with_context(|| {
                format!(
                    "building linked substrate {name:?} (this builder supports {:?})",
                    builder.supported()
                )
            })?;
            let (addr, boundary) = spawn_substrate_front(transport).await?;
            Ok(Front {
                addr,
                keep: FrontGuard::Bridge(boundary),
            })
        }
    }
}

async fn spawn_sidecar(program: &str, args: &[String], gateway: SocketAddr) -> Result<Front> {
    let mut child = Command::new(program)
        .args(args)
        .env(SIDECAR_TARGET_ENV, gateway.to_string())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("spawning sidecar {program:?}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("sidecar {program:?} has no stdout"))?;
    let mut lines = BufReader::new(stdout).lines();
    let addr = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while let Some(line) = lines.next_line().await? {
            if let Some(rest) = line.strip_prefix(FRONT_LINE) {
                return rest
                    .trim()
                    .parse::<SocketAddr>()
                    .map_err(|e| anyhow!("sidecar FRONT addr {rest:?}: {e}"));
            }
        }
        Err(anyhow!("sidecar {program:?} exited before announcing a FRONT"))
    })
    .await
    .map_err(|_| anyhow!("sidecar {program:?} did not announce a FRONT within 30s"))??;
    // Keep draining stdout so the child never blocks writing logs to a full pipe.
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    Ok(Front {
        addr,
        keep: FrontGuard::Child(child),
    })
}

/// Bind a local UDP front and shuttle datagrams between it and `substrate` (wrapped in
/// the paced/backpressured [`Substrate`] boundary; the queue is sized to the
/// substrate's BDP). quinn dials the returned addr and speaks ordinary UDP; the bytes
/// traverse the substrate. This is the shared bridge a sidecar uses to expose its
/// substrate as a UDP front (generalized from the original Nym client bridge). The
/// returned [`Substrate`] handle exposes the boundary metrics; keep it (and the
/// spawned pumps it owns) alive for the lifetime of the front.
pub async fn spawn_substrate_front(
    substrate: Arc<dyn MixTransport>,
) -> Result<(SocketAddr, Arc<Substrate>)> {
    let depth = crate::client::bdp_packets(&substrate.oracle());
    let sub = Substrate::new(substrate, depth);
    let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let front_addr = front.local_addr()?;
    // Learned address of the local quinn client (set on its first packet out).
    let peer: Arc<Mutex<Option<SocketAddr>>> = Arc::new(Mutex::new(None));
    // quinn -> substrate (backpressured paced enqueue).
    {
        let (front, sub, peer) = (front.clone(), sub.clone(), peer.clone());
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            while let Ok((n, addr)) = front.recv_from(&mut buf).await {
                *peer.lock().await = Some(addr);
                if sub.send(buf[..n].to_vec()).await.is_err() {
                    break;
                }
            }
        });
    }
    // substrate -> quinn (replies coming back through the mix).
    {
        let (front, sub, peer) = (front.clone(), sub.clone(), peer.clone());
        tokio::spawn(async move {
            while let Ok(dg) = sub.recv().await {
                if let Some(addr) = *peer.lock().await {
                    let _ = front.send_to(&dg, addr).await;
                }
            }
        });
    }
    Ok((front_addr, sub))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Env-driven parsing is global state; serialize the cases under one test.
    #[test]
    fn from_env_parses_choices() {
        // Helper to run with a clean, controlled env.
        fn with_env<T>(vars: &[(&str, Option<&str>)], f: impl FnOnce() -> T) -> T {
            let saved: Vec<(String, Option<String>)> = vars
                .iter()
                .map(|(k, _)| (k.to_string(), std::env::var(k).ok()))
                .collect();
            for (k, v) in vars {
                match v {
                    Some(val) => std::env::set_var(k, val),
                    None => std::env::remove_var(k),
                }
            }
            let out = f();
            for (k, v) in saved {
                match v {
                    Some(val) => std::env::set_var(&k, val),
                    None => std::env::remove_var(&k),
                }
            }
            out
        }

        let keys = ["QUICMIX_SUBSTRATE", "QUICMIX_SIDECAR_FRONT", "QUICMIX_SIDECAR_CMD"];

        // default + explicit direct
        with_env(
            &keys.map(|k| (k, None)),
            || assert_eq!(SubstrateChoice::from_env().unwrap(), SubstrateChoice::Direct),
        );
        with_env(&[("QUICMIX_SUBSTRATE", Some("direct")), ("QUICMIX_SIDECAR_FRONT", None), ("QUICMIX_SIDECAR_CMD", None)], || {
            assert_eq!(SubstrateChoice::from_env().unwrap(), SubstrateChoice::Direct)
        });

        // nym + fixed sidecar front
        with_env(
            &[("QUICMIX_SUBSTRATE", Some("nym")), ("QUICMIX_SIDECAR_FRONT", Some("127.0.0.1:7000")), ("QUICMIX_SIDECAR_CMD", None)],
            || {
                assert_eq!(
                    SubstrateChoice::from_env().unwrap(),
                    SubstrateChoice::SidecarAt("127.0.0.1:7000".parse().unwrap())
                )
            },
        );

        // tor + spawn command
        with_env(
            &[("QUICMIX_SUBSTRATE", Some("tor")), ("QUICMIX_SIDECAR_FRONT", None), ("QUICMIX_SIDECAR_CMD", Some("quicmix-tor-sidecar --hops 3"))],
            || {
                assert_eq!(
                    SubstrateChoice::from_env().unwrap(),
                    SubstrateChoice::SidecarCmd {
                        name: "tor".into(),
                        program: "quicmix-tor-sidecar".into(),
                        args: vec!["--hops".into(), "3".into()],
                    }
                )
            },
        );

        // non-direct with no sidecar config => a linked (Named) substrate, resolved
        // by an injected builder later (an error only if nothing can build it).
        with_env(
            &[("QUICMIX_SUBSTRATE", Some("katzenpost")), ("QUICMIX_SIDECAR_FRONT", None), ("QUICMIX_SIDECAR_CMD", None)],
            || {
                assert_eq!(
                    SubstrateChoice::from_env().unwrap(),
                    SubstrateChoice::Named { name: "katzenpost".into() }
                )
            },
        );
    }

    #[tokio::test]
    async fn named_without_builder_errors() {
        let gateway = "203.0.113.7:4433".parse().unwrap();
        let choice = SubstrateChoice::Named { name: "katzenpost".into() };
        assert!(resolve(&choice, gateway, None).await.is_err());
    }
}
