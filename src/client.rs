//! Client-side pluggability.
//!
//! The substrate side is plugged via [`crate::MixTransport`] / [`crate::tor`].
//! This module is the *client* side: the swappable knobs that sit above any
//! substrate — **where the oracle comes from**, **which congestion plan** to run,
//! and the **rotation policy** — bundled into a [`ClientProfile`] and wired into a
//! QUIC stack.
//!
//! What's a real plug here:
//! - **Oracle source** — [`FixedOracle`] (exact: emulator / self-hosted config)
//!   vs [`MeasuredOracle`] (online [`crate::oracle::OracleEstimator`]).
//! - **Congestion plan** — [`Congestion`] (`Stock` / `Timers` / `Quicmix`), each
//!   producing a `quinn::TransportConfig`.
//! - **Rotation policy** — [`RotationPolicy`] (pool size, …).
//!
//! What's still a documented swap (not code here): the **QUIC stack itself**
//! (quinn ↔ s2n-quic). `Congestion::transport` targets quinn today; a `QuicStack`
//! trait would abstract endpoint construction so s2n-quic's pluggable CC
//! `Provider` could drop in. quinn is the only stack wired so far.

use crate::oracle::OracleEstimator;
use crate::sched::OracleCc;
use crate::OracleParams;
use quinn::{IdleTimeout, TransportConfig};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Where the scheduler's [`OracleParams`] come from.
pub trait OracleSource: Send + Sync {
    fn params(&self) -> OracleParams;
}

/// Exact params — the emulator, or a self-hosted topology's configured policy.
pub struct FixedOracle(pub OracleParams);
impl OracleSource for FixedOracle {
    fn params(&self) -> OracleParams {
        self.0
    }
}

/// Measured params from an online estimator (a real substrate, e.g. Nym).
pub struct MeasuredOracle(pub Arc<Mutex<OracleEstimator>>);
impl OracleSource for MeasuredOracle {
    fn params(&self) -> OracleParams {
        self.0.lock().expect("oracle estimator lock").estimate()
    }
}

/// Bandwidth-delay product (bytes) for sizing the fixed window. ~BDP so quicmix
/// paces at ≈ the mix rate rather than overdriving the finite egress buffer.
pub fn bdp_bytes(p: &OracleParams) -> u64 {
    let rtt = (p.mean_hop_delay * p.hops * 2).as_secs_f64();
    if p.slot_interval.is_zero() {
        16 * 1024 * 1024 // uncapped emulator: large window
    } else {
        let rate = p.mtu as f64 / p.slot_interval.as_secs_f64(); // bytes/s
        ((rate * rtt) as u64).max(p.mtu as u64 * 2)
    }
}

/// The congestion plan — the client-side plug for "how to drive QUIC's CC".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Congestion {
    /// Stock QUIC (CUBIC). Baseline — collapses over a mixnet.
    Stock,
    /// Reorder-tolerant loss detection only (still backs off on real drops).
    Timers,
    /// quicmix: BDP window + loss ignored as congestion + tolerant timers.
    Quicmix,
}

impl Congestion {
    pub fn label(self) -> &'static str {
        match self {
            Congestion::Stock => "cubic",
            Congestion::Timers => "cubic+timers",
            Congestion::Quicmix => "quicmix",
        }
    }

    /// Build a quinn `TransportConfig` for this plan against `p`.
    pub fn transport(self, p: &OracleParams) -> Arc<TransportConfig> {
        let mut t = TransportConfig::default();
        let tolerant = |t: &mut TransportConfig| {
            // Reorder/jitter tolerance derived from the *measured* mix model, so a
            // merely-delayed packet isn't read as a drop (the spurious-retransmit
            // problem) while real drops are still recovered. `loss_time_threshold`
            // scales with the path's jitter (hops/delay) — dynamic, not a magic
            // constant; going to "never" would stall on the real drop rate.
            t.initial_rtt(p.rtt());
            t.packet_threshold(1000);
            t.time_threshold(p.loss_time_threshold());
            // Real mixnet RTTs are *seconds* (Nym mainnet p50 ≈ 2.8 s measured by
            // `realprobe`), and a session is many round-trips. Keep the connection
            // alive far longer than one RTT so it doesn't idle out mid-session.
            // Floored at 30 s so the ms-scale emulator is unaffected (its transfers
            // finish in well under that).
            let idle = std::cmp::max(Duration::from_secs(30), p.rtt() * 40);
            if let Ok(t_idle) = IdleTimeout::try_from(idle) {
                t.max_idle_timeout(Some(t_idle));
            }
        };
        match self {
            Congestion::Stock => {}
            Congestion::Timers => tolerant(&mut t),
            Congestion::Quicmix => {
                tolerant(&mut t);
                t.congestion_controller_factory(Arc::new(OracleCc {
                    window: bdp_bytes(p),
                }));
            }
        }
        Arc::new(t)
    }
}

/// Rotation policy knobs (the client-side plug for unlinkable rotation).
#[derive(Clone, Copy, Debug)]
pub struct RotationPolicy {
    pub pool_size: usize,
}

impl Default for RotationPolicy {
    fn default() -> Self {
        Self { pool_size: 4 }
    }
}

/// The pluggable client-side bundle: oracle source + congestion plan + rotation
/// policy. Substrate-agnostic; combine with any [`crate::MixTransport`].
pub struct ClientProfile {
    pub oracle: Box<dyn OracleSource>,
    pub congestion: Congestion,
    pub rotation: RotationPolicy,
}

impl ClientProfile {
    pub fn transport(&self) -> Arc<TransportConfig> {
        self.congestion.transport(&self.oracle.params())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn p() -> OracleParams {
        OracleParams {
            hops: 3,
            mean_hop_delay: Duration::from_millis(5),
            drop_prob: 0.02,
            slot_interval: Duration::from_millis(1),
            mtu: 1200,
        }
    }

    #[test]
    fn oracle_sources_interchangeable() {
        let fixed = FixedOracle(p());
        assert_eq!(fixed.params(), p());

        let est = Arc::new(Mutex::new(OracleEstimator::new(3, 1200, Duration::from_millis(1))));
        est.lock().unwrap().record_rtt(Duration::from_millis(60));
        let measured = MeasuredOracle(est);
        // hops/mtu structural; delay measured — just confirm it produces params.
        assert_eq!(measured.params().hops, 3);
    }

    #[test]
    fn every_congestion_plan_builds_and_quicmix_sizes_window() {
        for c in [Congestion::Stock, Congestion::Timers, Congestion::Quicmix] {
            let _ = c.transport(&p()); // must not panic
        }
        assert!(bdp_bytes(&p()) >= 2 * 1200);
    }

    #[test]
    fn profile_produces_transport() {
        let profile = ClientProfile {
            oracle: Box::new(FixedOracle(p())),
            congestion: Congestion::Quicmix,
            rotation: RotationPolicy::default(),
        };
        let _ = profile.transport();
    }
}
