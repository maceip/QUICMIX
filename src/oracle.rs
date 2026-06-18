//! Online oracle estimation.
//!
//! On the emulator the oracle params are exact (we set them). On a real mixnet
//! they are **measured**: the client observes realized round-trips and ack/loss
//! counts and estimates the distribution the scheduler needs. This is the bridge
//! between the two substrates and the subject of the known-vs-measured comparison
//! — feed the scheduler exact params (emulator / self-hosted config) vs these
//! estimates and quantify the gap.

use crate::OracleParams;
use std::time::Duration;

/// Bounded recent-RTT window. Caps memory and keeps the estimate weighted toward
/// recent observations rather than every tick a long-lived connection survives.
const MAX_RTT_SAMPLES: usize = 256;

/// Accumulates observed path round-trips and ack/loss counts, then produces a
/// measured [`OracleParams`]. `hops`, `mtu`, and `slot_interval` are structural
/// facts known from the chosen route/policy; delay and drop are measured.
#[derive(Clone, Debug)]
pub struct OracleEstimator {
    rtt_ns: Vec<u128>,
    sent: u64,
    lost: u64,
    hops: u32,
    mtu: usize,
    slot_interval: Duration,
}

impl OracleEstimator {
    pub fn new(hops: u32, mtu: usize, slot_interval: Duration) -> Self {
        Self {
            rtt_ns: Vec::new(),
            sent: 0,
            lost: 0,
            hops: hops.max(1),
            mtu,
            slot_interval,
        }
    }

    /// Record one observed round-trip (e.g. a probe or first stream RTT). The buffer
    /// is a bounded recent window (`MAX_RTT_SAMPLES`): repeated sampling of the same
    /// long-lived connection can't grow memory without bound or let stale samples
    /// dominate the percentiles — recent observations win.
    pub fn record_rtt(&mut self, rtt: Duration) {
        if self.rtt_ns.len() >= MAX_RTT_SAMPLES {
            self.rtt_ns.remove(0); // drop oldest; window stays ≤ MAX_RTT_SAMPLES
        }
        self.rtt_ns.push(rtt.as_nanos());
    }

    /// Record cumulative ack/loss counts (e.g. from `Connection::stats()`).
    pub fn record_acks(&mut self, sent: u64, lost: u64) {
        self.sent = sent;
        self.lost = lost;
    }

    /// Number of RTT samples folded in so far.
    pub fn samples(&self) -> usize {
        self.rtt_ns.len()
    }

    /// (p50, p90, p99) of the observed RTT distribution — the measured spread the
    /// observability layer exports and the loss timer is derived from.
    pub fn rtt_percentiles(&self) -> (Duration, Duration, Duration) {
        if self.rtt_ns.is_empty() {
            return (Duration::ZERO, Duration::ZERO, Duration::ZERO);
        }
        let mut v = self.rtt_ns.clone();
        v.sort_unstable();
        let pick = |q: f64| -> Duration {
            let idx = (((v.len() - 1) as f64) * q).round() as usize;
            Duration::from_nanos(v[idx] as u64)
        };
        (pick(0.50), pick(0.90), pick(0.99))
    }

    /// Fold a live quinn connection's realized RTT + loss into the estimate — the
    /// bridge from a real connection's behaviour to the measured oracle.
    pub fn observe_connection(&mut self, conn: &quinn::Connection) {
        self.record_rtt(conn.rtt());
        let s = conn.stats();
        self.record_acks(s.path.sent_packets, s.path.lost_packets);
    }

    /// Median observed RTT in nanoseconds (robust to mix-delay outliers).
    fn median_rtt_ns(&self) -> u128 {
        if self.rtt_ns.is_empty() {
            return 0;
        }
        let mut v = self.rtt_ns.clone();
        v.sort_unstable();
        v[v.len() / 2]
    }

    /// Model-free jitter ratio (a high RTT percentile / median) — a measured
    /// `loss_time_threshold` for a real substrate, no Erlang assumption.
    pub fn jitter_ratio(&self) -> f32 {
        if self.rtt_ns.len() < 2 {
            return 0.0;
        }
        let mut v = self.rtt_ns.clone();
        v.sort_unstable();
        let med = v[v.len() / 2] as f32;
        let idx = ((v.len() as f32 * 0.999) as usize).min(v.len() - 1);
        let p999 = v[idx] as f32;
        if med > 0.0 {
            (p999 / med).max(1.5)
        } else {
            0.0
        }
    }

    /// Produce measured oracle params. `mean_hop_delay` is derived from the
    /// one-way path delay (≈ median RTT / 2) spread over the hops.
    pub fn estimate(&self) -> OracleParams {
        let one_way_ns = self.median_rtt_ns() / 2;
        let per_hop_ns = one_way_ns / self.hops as u128;
        let drop_prob = if self.sent > 0 {
            self.lost as f64 / self.sent as f64
        } else {
            0.0
        };
        OracleParams {
            hops: self.hops,
            mean_hop_delay: Duration::from_nanos(per_hop_ns as u64),
            drop_prob,
            slot_interval: self.slot_interval,
            mtu: self.mtu,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimates_per_hop_delay_and_drop() {
        // True path: 3 hops, 10ms/hop → 30ms one-way → 60ms RTT.
        let mut est = OracleEstimator::new(3, 1200, Duration::from_millis(1));
        for _ in 0..50 {
            est.record_rtt(Duration::from_millis(60));
        }
        est.record_acks(1000, 20);
        let p = est.estimate();
        assert_eq!(p.hops, 3);
        // ~10ms/hop within a millisecond.
        let hop_ms = p.mean_hop_delay.as_secs_f64() * 1e3;
        assert!((hop_ms - 10.0).abs() < 1.0, "hop≈10ms, got {hop_ms}");
        assert!((p.drop_prob - 0.02).abs() < 1e-9, "drop≈2%");
        assert_eq!(p.mtu, 1200);
    }

    #[test]
    fn empty_estimator_is_safe() {
        let p = OracleEstimator::new(3, 1200, Duration::ZERO).estimate();
        assert_eq!(p.mean_hop_delay, Duration::ZERO);
        assert_eq!(p.drop_prob, 0.0);
    }
}
