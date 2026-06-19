//! Observability contract.
//!
//! One structured [`Snapshot`] of stable, documented metric names rendered as
//! Prometheus exposition text — the wire contract a scrape/dashboard codes against.
//! It folds together the three boundaries quicmix already measures:
//!
//! - **substrate boundary** ([`quicmix::substrate::Metrics`]) — sent/recv/send_errors/
//!   dropped counters plus queue-depth and enqueue-latency gauges.
//! - **QUIC path** (live [`quinn::Connection`] stats) — sent/lost packets, lost
//!   bytes, congestion events. quinn exposes **no** standalone retransmit counter;
//!   in QUIC a *lost* packet's frames are exactly what get retransmitted, so
//!   `lost_packets`/`lost_bytes` **are** the retransmission signals (named honestly
//!   rather than fabricating a separate number).
//! - **oracle** ([`quicmix::oracle::OracleEstimator`]) — measured RTT p50/p90/p99.
//! - **circuits** ([`crate::proxy::WarmPool`]) — built/retired/build_errors counters
//!   and the ready gauge.
//!
//! Everything is composable: a process fills the parts it owns (the proxy owns
//! quic+oracle+circuits; a relay owns the substrate boundary) and renders the union.

use quicmix::substrate::MetricsSnapshot as SubstrateMetrics;
use std::fmt::Write as _;
use std::time::Duration;

/// QUIC path counters summed across the currently-live circuits. Each connection's
/// `path` stats are cumulative since that connection was built, so a fresh sum is a
/// correct point-in-time total for the pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QuicStats {
    pub sent_packets: u64,
    pub lost_packets: u64,
    pub lost_bytes: u64,
    pub congestion_events: u64,
}

impl QuicStats {
    /// Fold one live connection's path stats into the running sum.
    pub fn observe(&mut self, conn: &quinn::Connection) {
        let p = conn.stats().path;
        self.sent_packets += p.sent_packets;
        self.lost_packets += p.lost_packets;
        self.lost_bytes += p.lost_bytes;
        self.congestion_events += p.congestion_events;
    }
}

/// A point-in-time, structured view of every exported metric.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Snapshot {
    // substrate boundary
    pub substrate_sent_total: u64,
    pub substrate_recv_total: u64,
    pub substrate_send_errors_total: u64,
    pub substrate_dropped_total: u64,
    pub substrate_queue_depth: u64,
    pub substrate_enqueue_latency_us: u64,
    // quic path
    pub quic_sent_packets_total: u64,
    pub quic_lost_packets_total: u64,
    pub quic_lost_bytes_total: u64,
    pub quic_congestion_events_total: u64,
    // oracle (measured RTT distribution)
    pub oracle_rtt_p50_seconds: f64,
    pub oracle_rtt_p90_seconds: f64,
    pub oracle_rtt_p99_seconds: f64,
    // circuits
    pub circuits_built_total: u64,
    pub circuits_retired_total: u64,
    pub circuits_build_errors_total: u64,
    pub circuits_ready: u64,
}

impl Snapshot {
    /// Merge the substrate-boundary counters/gauges.
    pub fn with_substrate(mut self, m: &SubstrateMetrics) -> Self {
        self.substrate_sent_total = m.sent;
        self.substrate_recv_total = m.received;
        self.substrate_send_errors_total = m.send_errors;
        self.substrate_dropped_total = m.dropped;
        self.substrate_queue_depth = m.queue_depth as u64;
        self.substrate_enqueue_latency_us = m.enqueue_latency_us;
        self
    }

    /// Merge the aggregated QUIC path stats.
    pub fn with_quic(mut self, q: QuicStats) -> Self {
        self.quic_sent_packets_total = q.sent_packets;
        self.quic_lost_packets_total = q.lost_packets;
        self.quic_lost_bytes_total = q.lost_bytes;
        self.quic_congestion_events_total = q.congestion_events;
        self
    }

    /// Merge the measured RTT percentiles (as seconds).
    pub fn with_oracle_rtt(mut self, p50: Duration, p90: Duration, p99: Duration) -> Self {
        self.oracle_rtt_p50_seconds = p50.as_secs_f64();
        self.oracle_rtt_p90_seconds = p90.as_secs_f64();
        self.oracle_rtt_p99_seconds = p99.as_secs_f64();
        self
    }

    /// Merge the circuit-pool counters and the ready gauge.
    pub fn with_circuits(mut self, built: u64, retired: u64, build_errors: u64, ready: u64) -> Self {
        self.circuits_built_total = built;
        self.circuits_retired_total = retired;
        self.circuits_build_errors_total = build_errors;
        self.circuits_ready = ready;
        self
    }

    /// Render as Prometheus exposition text (`# HELP` / `# TYPE` / value per metric).
    /// The set of names here **is** the observability contract.
    pub fn render_prometheus(&self) -> String {
        fn u(v: u64) -> String { v.to_string() }
        fn f(v: f64) -> String { format!("{v}") }

        // (kind, name, help, value) for every metric — the set of names here **is**
        // the observability contract.
        let metrics: [(&str, &str, &str, String); 18] = [
            ("counter", "substrate_sent_total", "datagrams handed to the inner substrate", u(self.substrate_sent_total)),
            ("counter", "substrate_recv_total", "datagrams received from the inner substrate", u(self.substrate_recv_total)),
            ("counter", "substrate_send_errors_total", "inner-substrate send failures (typed SubstrateError)", u(self.substrate_send_errors_total)),
            ("counter", "substrate_dropped_total", "datagrams dropped by the lossy send path under backpressure", u(self.substrate_dropped_total)),
            ("counter", "quic_sent_packets_total", "QUIC packets sent across live circuits", u(self.quic_sent_packets_total)),
            ("counter", "quic_lost_packets_total", "QUIC packets declared lost (the retransmission trigger)", u(self.quic_lost_packets_total)),
            ("counter", "quic_lost_bytes_total", "QUIC bytes in lost packets (the volume retransmitted)", u(self.quic_lost_bytes_total)),
            ("counter", "quic_congestion_events_total", "QUIC congestion events across live circuits", u(self.quic_congestion_events_total)),
            ("counter", "circuits_built_total", "circuits ever built (pre-warm + refill + rotation)", u(self.circuits_built_total)),
            ("counter", "circuits_retired_total", "circuits retired into draining for rotation/age/use", u(self.circuits_retired_total)),
            ("counter", "circuits_build_errors_total", "failed circuit builds during refill", u(self.circuits_build_errors_total)),
            ("gauge", "substrate_queue_depth", "current paced send-queue depth", u(self.substrate_queue_depth)),
            ("gauge", "substrate_enqueue_latency_us", "age (microseconds) of the most-recently-sent queued datagram", u(self.substrate_enqueue_latency_us)),
            ("gauge", "circuits_ready", "warm, non-draining circuits ready to serve", u(self.circuits_ready)),
            ("gauge", "oracle_rtt_p50_seconds", "measured RTT median", f(self.oracle_rtt_p50_seconds)),
            ("gauge", "oracle_rtt_p90_seconds", "measured RTT 90th percentile", f(self.oracle_rtt_p90_seconds)),
            ("gauge", "oracle_rtt_p99_seconds", "measured RTT 99th percentile", f(self.oracle_rtt_p99_seconds)),
            ("gauge", "quic_loss_ratio", "lost/sent packets across live circuits", f(self.loss_ratio())),
        ];

        let mut out = String::new();
        for (kind, name, help, val) in metrics {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} {kind}");
            let _ = writeln!(out, "{name} {val}");
        }
        out
    }

    fn loss_ratio(&self) -> f64 {
        if self.quic_sent_packets_total == 0 {
            0.0
        } else {
            self.quic_lost_packets_total as f64 / self.quic_sent_packets_total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_renders_every_named_metric() {
        let snap = Snapshot::default()
            .with_quic(QuicStats {
                sent_packets: 1000,
                lost_packets: 7,
                lost_bytes: 8400,
                congestion_events: 2,
            })
            .with_oracle_rtt(
                Duration::from_millis(700),
                Duration::from_millis(1800),
                Duration::from_millis(2800),
            )
            .with_circuits(12, 3, 1, 4);

        let text = snap.render_prometheus();
        // Every name in the contract must appear, exactly once as a value line.
        for name in [
            "substrate_sent_total",
            "substrate_recv_total",
            "substrate_send_errors_total",
            "substrate_dropped_total",
            "substrate_queue_depth",
            "substrate_enqueue_latency_us",
            "quic_sent_packets_total",
            "quic_lost_packets_total",
            "quic_lost_bytes_total",
            "quic_congestion_events_total",
            "oracle_rtt_p50_seconds",
            "oracle_rtt_p90_seconds",
            "oracle_rtt_p99_seconds",
            "circuits_built_total",
            "circuits_retired_total",
            "circuits_build_errors_total",
            "circuits_ready",
        ] {
            let value_lines =
                text.lines().filter(|l| l.starts_with(&format!("{name} "))).count();
            assert_eq!(value_lines, 1, "metric `{name}` must render exactly one value line");
            assert!(text.contains(&format!("# TYPE {name} ")), "metric `{name}` needs a # TYPE");
        }

        // Real values propagate.
        assert!(text.contains("quic_lost_packets_total 7"));
        assert!(text.contains("circuits_built_total 12"));
        assert!(text.contains("circuits_ready 4"));
        assert!(text.contains("oracle_rtt_p50_seconds 0.7"));
    }

    #[test]
    fn quic_stats_sum_across_connections_is_additive() {
        // Two synthetic per-conn cumulative readings sum into the pool total.
        let mut q = QuicStats::default();
        // simulate two connections' cumulative stats by direct fold
        q.sent_packets += 500;
        q.lost_packets += 3;
        let mut q2 = q;
        q2.sent_packets += 500;
        q2.lost_packets += 4;
        assert_eq!(q2.sent_packets, 1000);
        assert_eq!(q2.lost_packets, 7);
    }
}
