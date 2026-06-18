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

/// One BDP expressed in **packets** (floored at 16) — the single source of truth for
/// sizing a bounded egress/relay queue from the oracle (used by the relay boundary
/// and the demo/bench harnesses), so the BDP-depth rule lives in exactly one place.
pub fn bdp_packets(p: &OracleParams) -> usize {
    (bdp_bytes(p) / p.mtu.max(1) as u64).max(16) as usize
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

/// The reorder/jitter-tolerant base `TransportConfig`, shared by every non-`Stock`
/// plan. Tolerance is derived from the *measured* mix model, so a merely-delayed
/// packet isn't read as a drop (the spurious-retransmit problem) while real drops are
/// still recovered. `time_threshold` scales with the path's jitter (hops/delay) —
/// dynamic, not a magic constant; going to "never" would stall on the real drop rate.
/// Real mixnet RTTs are *seconds* (Nym mainnet p50 ≈ 2.8 s measured by `realprobe`)
/// and a session is many round-trips, so the idle timeout is kept far above one RTT
/// (≥ 30 s, so the ms-scale emulator is unaffected) to avoid idling out mid-session.
fn tolerant(p: &OracleParams, time_threshold: f32) -> TransportConfig {
    let mut t = TransportConfig::default();
    t.initial_rtt(p.rtt());
    t.packet_threshold(1000);
    t.time_threshold(time_threshold);
    let idle = std::cmp::max(Duration::from_secs(30), p.rtt() * 40);
    if let Ok(t_idle) = IdleTimeout::try_from(idle) {
        t.max_idle_timeout(Some(t_idle));
    }
    t
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
        match self {
            Congestion::Stock => Arc::new(TransportConfig::default()),
            Congestion::Timers => Arc::new(tolerant(p, p.loss_time_threshold())),
            Congestion::Quicmix => quicmix_transport_with_jitter(p, 0.0),
        }
    }
}

/// Build the oracle-fed `Quicmix` transport (tolerant timers + the BDP-windowed
/// [`OracleCc`]) with an explicit, **measured** loss-detection threshold (a jitter
/// percentile ratio from real RTTs). `jitter < 1.5` falls back to the hops-derived
/// structural default (too few samples to trust). Used to rebuild a circuit's
/// transport from the latest measured oracle on rotation.
pub fn quicmix_transport_with_jitter(p: &OracleParams, jitter: f32) -> Arc<TransportConfig> {
    let threshold = if jitter >= 1.5 { jitter } else { p.loss_time_threshold() };
    let mut t = tolerant(p, threshold);
    t.congestion_controller_factory(Arc::new(OracleCc { window: bdp_bytes(p) }));
    Arc::new(t)
}

/// Drives the oracle-fed CC from **live measurements**. Fold real connections in via
/// [`MeasuredCc::observe`] (their realized RTT + loss feed an [`OracleEstimator`]);
/// each new circuit's `TransportConfig` is rebuilt from the latest measured oracle
/// via [`MeasuredCc::transport`], which **logs** every change to the oracle. This is
/// the bridge from real connection behaviour to the next circuit's CC.
pub struct MeasuredCc {
    est: Mutex<OracleEstimator>,
    last: Mutex<Option<OracleParams>>,
}

impl MeasuredCc {
    /// `hops`/`mtu`/`slot_interval` are the structural facts of the chosen route;
    /// delay and drop are measured from observed connections.
    pub fn new(hops: u32, mtu: usize, slot_interval: Duration) -> Self {
        Self {
            est: Mutex::new(OracleEstimator::new(hops, mtu, slot_interval)),
            last: Mutex::new(None),
        }
    }

    /// Fold a live connection's realized RTT + loss into the measurement.
    pub fn observe(&self, conn: &quinn::Connection) {
        self.est.lock().expect("oracle lock").observe_connection(conn);
    }

    /// Record a directly-observed round-trip (e.g. a probe).
    pub fn record_rtt(&self, rtt: Duration) {
        self.est.lock().expect("oracle lock").record_rtt(rtt);
    }

    /// The current measured oracle.
    pub fn current(&self) -> OracleParams {
        self.est.lock().expect("oracle lock").estimate()
    }

    /// (p50, p90, p99) measured RTT — for the observability layer.
    pub fn rtt_percentiles(&self) -> (Duration, Duration, Duration) {
        self.est.lock().expect("oracle lock").rtt_percentiles()
    }

    /// Number of RTT samples folded in.
    pub fn samples(&self) -> usize {
        self.est.lock().expect("oracle lock").samples()
    }

    /// Build the next circuit's oracle-fed transport from the **latest** measured
    /// oracle (measured jitter drives the loss timer), logging any change old→new.
    pub fn transport(&self) -> Arc<TransportConfig> {
        let (p, jitter) = {
            let e = self.est.lock().expect("oracle lock");
            (e.estimate(), e.jitter_ratio())
        };
        {
            let mut last = self.last.lock().expect("oracle last lock");
            if last.as_ref() != Some(&p) {
                if let Some(old) = last.as_ref() {
                    eprintln!(
                        "[oracle] measured change: rtt {:.0}ms->{:.0}ms  drop {:.3}->{:.3}  jitter×{:.2}  (n={})",
                        old.rtt().as_secs_f64() * 1e3,
                        p.rtt().as_secs_f64() * 1e3,
                        old.drop_prob,
                        p.drop_prob,
                        jitter,
                        self.samples()
                    );
                }
                *last = Some(p);
            }
        }
        quicmix_transport_with_jitter(&p, jitter)
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
