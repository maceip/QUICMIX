//! quicmix — QUIC transport adaptation for metadata-private mixnets.
//!
//! See `DESIGN.md`. Two contributions:
//!   * Area A — oracle-driven scheduler (mixnet-aware "congestion control").
//!   * Area B — unlinkable circuit rotation (pre-warmed pool + e2e rebind).
//!
//! This crate is independent from the root `freevpn` mixnet, which is left
//! exactly as-is. quicmix is a *transport seam*, not a new anonymity primitive.

use std::time::Duration;

pub mod client;
pub mod directory;
pub mod emulator;
pub mod ingress;
pub mod metrics;
pub mod node;
pub mod oracle;
pub mod proxy;
pub mod relay;
pub mod rotation;
pub mod sched;
pub mod striped;
pub mod substrate;
pub mod tor;

/// The kind of anonymity substrate. quicmix targets **datagram** mixnets only;
/// stream substrates (Tor/onion) are a documented non-goal — quicmix is a no-op
/// over a reliable stream (its CC and rotation belong to the substrate there).
/// See `PLUGGABILITY.md`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubstrateKind {
    /// Unreliable, reordering **datagram** pipe (a mixnet: `EmulatedMixnet`, Nym,
    /// Katzenpost, other Loopix/Sphinx-style nets). QUIC runs natively; quicmix's
    /// oracle-fed congestion control and unlinkable rotation apply. Plug point:
    /// [`MixTransport`].
    Datagram,
    /// Reliable, ordered **byte stream** (Tor via SOCKS5/arti). Joins the datagram
    /// round-robin only via length-prefix framing ([`tor::StreamDatagram`]), where
    /// it head-of-line-blocks and quicmix's CC does not govern the path. A
    /// fast/compatible *slow leg*, not a peer of the datagram substrates.
    Stream,
}

/// Parameters the mix layer exposes to the scheduler (Area A).
///
/// On the [`emulator::EmulatedMixnet`] these are **exact** (we set them). On a
/// real Nym deployment the policy params are known (published on mainnet, or set
/// by us on a self-hosted AWS topology) while *realized* delay/loss are
/// **measured empirically**; a deployed scheduler additionally estimates them
/// online, and we evaluate its sensitivity to that error. The scheduler must
/// only ever use *public/aggregate* values from here; it must never key timing
/// decisions on "which flow is mine" (that would make it a timing side channel).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OracleParams {
    /// Number of mix hops on the path.
    pub hops: u32,
    /// Mean per-hop delay (exponential). Total path delay ≈ Erlang(hops, mean).
    pub mean_hop_delay: Duration,
    /// End-to-end per-datagram drop probability (cover displacement, policy drops).
    /// These are *anonymity* drops — the scheduler must ARQ-recover them, not
    /// treat them as congestion.
    pub drop_prob: f64,
    /// Constant-rate budget: at most one cell per this interval per circuit. The
    /// scheduler conforms to this rather than probing past it.
    pub slot_interval: Duration,
    /// Usable payload bytes per cell (one QUIC datagram should fit in one cell).
    pub mtu: usize,
}

impl OracleParams {
    /// Mean round-trip time over the path (forward + return).
    pub fn rtt(&self) -> Duration {
        self.mean_hop_delay * self.hops * 2
    }

    /// Loss-detection time multiplier **derived from the measured jitter**, so a
    /// merely-mix-delayed packet isn't mistaken for a drop (the spurious-PTO
    /// problem) while real drops are still recovered.
    ///
    /// Round-trip delay is a sum of `2*hops` per-hop delays — Erlang(2·hops,
    /// mean_hop_delay), coefficient of variation `1/sqrt(2·hops)`. We set the
    /// threshold to cover ≈ the p99.9 of that relative to the mean
    /// (`1 + z/sqrt(2·hops)`, z≈3.1). More hops ⇒ lower relative jitter ⇒ tighter
    /// threshold; fewer hops ⇒ wider. This replaces the old hardcoded `3.0`:
    /// it now scales with the substrate's current measured delay/hops. (A
    /// `MeasuredOracle` can compute this from observed RTT percentiles directly —
    /// see `oracle::OracleEstimator::jitter_ratio` — for a model-free version.)
    pub fn loss_time_threshold(&self) -> f32 {
        let n = (2 * self.hops.max(1)) as f32;
        (1.0 + 3.1 / n.sqrt()).max(1.5)
    }
}

/// A datagram pipe with a known/estimated timing model.
///
/// Implemented by [`emulator::EmulatedMixnet`], the (spec'd) Nym/Katzenpost
/// bindings, the Tor stream-adapter, and [`striped::Striped`]. Object-safe (via
/// `async_trait`) so heterogeneous substrates can be combined as
/// `Arc<dyn MixTransport>` and round-robined.
/// Typed substrate-boundary errors. Real substrate failures map into these instead
/// of being silently dropped (`let _ = ...`); [`substrate::Substrate`] increments
/// metrics and surfaces them to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubstrateError {
    /// The substrate is gone (stream / socket / client closed).
    Closed,
    /// An operation exceeded its deadline.
    Timeout,
    /// A received frame/message could not be parsed.
    Malformed,
    /// Authentication/authorization with the substrate failed (e.g. HTTP 401).
    AuthFailed,
    /// The remote/gateway rejected the request (e.g. HTTP 5xx, a policy drop).
    RemoteRejected,
    /// The send queue is full — the caller is outrunning the paced rate.
    Backpressure,
    /// An underlying I/O / library error, with context.
    Io(String),
}

impl std::fmt::Display for SubstrateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubstrateError::Closed => write!(f, "substrate closed"),
            SubstrateError::Timeout => write!(f, "substrate timeout"),
            SubstrateError::Malformed => write!(f, "malformed substrate frame"),
            SubstrateError::AuthFailed => write!(f, "substrate auth failed"),
            SubstrateError::RemoteRejected => write!(f, "substrate remote rejected"),
            SubstrateError::Backpressure => write!(f, "substrate send queue full (backpressure)"),
            SubstrateError::Io(e) => write!(f, "substrate io: {e}"),
        }
    }
}
impl std::error::Error for SubstrateError {}

#[async_trait::async_trait]
pub trait MixTransport: Send + Sync {
    /// Datagram substrate (the emulator, Nym, and other datagram mixnets).
    fn kind(&self) -> SubstrateKind {
        SubstrateKind::Datagram
    }
    /// The timing model the scheduler reads instead of inferring from RTT/loss.
    fn oracle(&self) -> OracleParams;
    /// Submit one datagram into the mix (infallible/legacy — see [`MixTransport::try_send`]).
    async fn send(&self, datagram: Vec<u8>);
    /// Receive the next datagram out of the mix (after delay/drop/reorder).
    async fn recv(&self) -> Option<Vec<u8>>;

    /// **Fallible send.** Real substrates override this to map their library/API
    /// errors into a typed [`SubstrateError`]; production paths call this so a
    /// failed send is never silently discarded. The default delegates to the
    /// infallible [`MixTransport::send`] (in-process transports can't fail).
    async fn try_send(&self, datagram: Vec<u8>) -> Result<(), SubstrateError> {
        self.send(datagram).await;
        Ok(())
    }
    /// **Fallible receive.** Distinguishes a real datagram from a closed/failed
    /// substrate. Real substrates override this to map errors; the default maps a
    /// `None` from [`MixTransport::recv`] to [`SubstrateError::Closed`].
    async fn try_recv(&self) -> Result<Vec<u8>, SubstrateError> {
        self.recv().await.ok_or(SubstrateError::Closed)
    }
}
