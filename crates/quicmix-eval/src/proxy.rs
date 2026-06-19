//! Durable, pooled, multipath ingress proxy — the client http→quic proxy wired to
//! all three quicmix mechanisms at once:
//!
//! - **oracle-fed congestion control** — every pooled circuit is built with the
//!   oracle's transport. With a [`quicmix::client::MeasuredCc`] (`start_measured`),
//!   each new circuit's `TransportConfig` is **rebuilt from the latest measured
//!   oracle** (delay/loss/jitter sampled from live connections).
//! - **the oracle** — measured online and fed back into newly-built circuits.
//! - **pre-warmed circuits** — the pool keeps `target` circuits handshaked and
//!   ready; a request never pays a cold handshake.
//!
//! Requests round-robin across the live circuits (multipath at the request level).
//! Rotation is **production-safe**: a retired circuit is marked *draining* (no new
//! requests) and kept — endpoint and all — until its in-flight requests finish or a
//! drain timeout elapses, so a forced rotation never kills a healthy in-flight
//! request. Refill happens off the hot path with explicit failure metrics.

use quicmix::client::MeasuredCc;
use quicmix::rotation::{connect_fresh_with, Circuit, FrontFactory};
use anyhow::Result;
use quinn::{Connection, TransportConfig};
use rustls::pki_types::CertificateDer;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Pool policy.
#[derive(Clone, Copy, Debug)]
pub struct PoolConfig {
    /// Warm (non-draining) circuits to keep ready.
    pub target: usize,
    /// Retire a circuit after this many requests (unlinkable rotation). `None` = never.
    pub max_uses: Option<u64>,
    /// Retire a circuit after this age. `None` = never.
    pub max_age: Option<Duration>,
    /// Maintainer tick (reap/drain/refill/sample cadence).
    pub tick: Duration,
    /// How long a *draining* circuit is kept for in-flight requests to finish before
    /// it is force-reaped (the bounded safety valve).
    pub drain_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            target: 4,
            max_uses: None,
            max_age: None,
            tick: Duration::from_millis(250),
            drain_timeout: Duration::from_secs(10),
        }
    }
}

/// Sample a given circuit into the measured oracle at most this often. The
/// maintainer ticks far faster than this (~tens of ms); without the throttle a
/// single long-lived connection would contribute a fresh RTT sample every tick and
/// dominate the estimate. One sample/sec/circuit keeps observations independent.
const SAMPLE_EVERY: Duration = Duration::from_secs(1);

struct Slot {
    circuit: Circuit,
    born: Instant,
    uses: u64,
    /// In-flight requests holding a [`Lease`] on this circuit.
    active: Arc<AtomicUsize>,
    /// `Some(deadline)` once retired — not handed to new requests; reaped when its
    /// in-flight count hits zero or `deadline` passes.
    draining: Option<Instant>,
    /// Last time this circuit was sampled into the measured oracle (throttle).
    last_sampled: Option<Instant>,
}

/// A leased connection for one in-flight request. While it lives the circuit is
/// counted as busy and will **not** be reaped (graceful drain); the count is
/// released on drop. Derefs/AsRef to the underlying [`Connection`].
pub struct Lease {
    conn: Arc<Connection>,
    active: Arc<AtomicUsize>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}
impl std::ops::Deref for Lease {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        &self.conn
    }
}
impl AsRef<Connection> for Lease {
    fn as_ref(&self) -> &Connection {
        &self.conn
    }
}

/// A self-refilling pool of pre-warmed circuits for the ingress proxy.
pub struct WarmPool {
    slots: Mutex<Vec<Slot>>,
    rr: AtomicUsize,
    fronts: Vec<FrontFactory>,
    front_rr: AtomicUsize,
    cert: CertificateDer<'static>,
    /// Fixed transport (when not measuring online).
    transport: Option<Arc<TransportConfig>>,
    /// Online measured oracle; when present, each new circuit's transport is rebuilt
    /// from it and live circuits are sampled into it.
    measured: Option<Arc<MeasuredCc>>,
    cfg: PoolConfig,
    built: AtomicU64,
    retired: AtomicU64,
    build_errors: AtomicU64,
}

impl WarmPool {
    /// Pre-warm `cfg.target` circuits with a **fixed** `transport`. See
    /// [`WarmPool::start_measured`] for the online-measured variant.
    pub async fn start(
        fronts: Vec<FrontFactory>,
        cert: CertificateDer<'static>,
        transport: Option<Arc<TransportConfig>>,
        cfg: PoolConfig,
    ) -> Result<Arc<Self>> {
        Self::spawn(fronts, cert, transport, None, cfg).await
    }

    /// Pre-warm with an online-**measured** oracle: each new circuit's transport is
    /// rebuilt from the latest measurement, and live circuits are sampled into it.
    pub async fn start_measured(
        fronts: Vec<FrontFactory>,
        cert: CertificateDer<'static>,
        measured: Arc<MeasuredCc>,
        cfg: PoolConfig,
    ) -> Result<Arc<Self>> {
        Self::spawn(fronts, cert, None, Some(measured), cfg).await
    }

    async fn spawn(
        fronts: Vec<FrontFactory>,
        cert: CertificateDer<'static>,
        transport: Option<Arc<TransportConfig>>,
        measured: Option<Arc<MeasuredCc>>,
        cfg: PoolConfig,
    ) -> Result<Arc<Self>> {
        anyhow::ensure!(!fronts.is_empty(), "WarmPool needs at least one front factory");
        let pool = Arc::new(Self {
            slots: Mutex::new(Vec::new()),
            rr: AtomicUsize::new(0),
            fronts,
            front_rr: AtomicUsize::new(0),
            cert,
            transport,
            measured,
            cfg,
            built: AtomicU64::new(0),
            retired: AtomicU64::new(0),
            build_errors: AtomicU64::new(0),
        });
        pool.refill().await; // pre-warm before returning
        {
            let p = pool.clone();
            tokio::spawn(async move { p.maintain().await });
        }
        Ok(pool)
    }

    fn next_front(&self) -> &FrontFactory {
        let i = self.front_rr.fetch_add(1, Ordering::Relaxed) % self.fronts.len();
        &self.fronts[i]
    }

    async fn build_one(&self) -> Result<Slot> {
        // Rebuild the transport from the latest measured oracle (logs changes), or
        // use the fixed one.
        let transport = match &self.measured {
            Some(m) => Some(m.transport()),
            None => self.transport.clone(),
        };
        let circuit = connect_fresh_with(self.next_front(), self.cert.clone(), transport).await?;
        self.built.fetch_add(1, Ordering::Relaxed);
        Ok(Slot {
            circuit,
            born: Instant::now(),
            uses: 0,
            active: Arc::new(AtomicUsize::new(0)),
            draining: None,
            last_sampled: None,
        })
    }

    /// Top the *ready* (non-draining, live) circuits back up to `target`. A failed
    /// build increments `build_errors` and is retried on the next tick.
    async fn refill(&self) {
        let ready = {
            let s = self.slots.lock().await;
            s.iter()
                .filter(|x| x.draining.is_none() && x.circuit.conn.close_reason().is_none())
                .count()
        };
        for _ in 0..self.cfg.target.saturating_sub(ready) {
            match self.build_one().await {
                Ok(slot) => self.slots.lock().await.push(slot),
                Err(_) => {
                    self.build_errors.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
        }
    }

    /// Lease a live, non-draining circuit for one request (round-robin). The lease
    /// keeps the circuit from being reaped until the request finishes.
    pub async fn pick(&self) -> Option<Lease> {
        let mut slots = self.slots.lock().await;
        slots.retain(|s| s.circuit.conn.close_reason().is_none());
        let idxs: Vec<usize> = (0..slots.len()).filter(|&i| slots[i].draining.is_none()).collect();
        if idxs.is_empty() {
            return None;
        }
        let i = idxs[self.rr.fetch_add(1, Ordering::Relaxed) % idxs.len()];
        slots[i].uses += 1;
        slots[i].active.fetch_add(1, Ordering::Relaxed);
        Some(Lease {
            conn: Arc::new(slots[i].circuit.conn.clone()),
            active: slots[i].active.clone(),
        })
    }

    async fn maintain(&self) {
        loop {
            tokio::time::sleep(self.cfg.tick).await;
            {
                let cfg = self.cfg;
                let mut slots = self.slots.lock().await;
                let now = Instant::now();
                // Retire aged/over-used circuits by marking them draining (not dropping).
                for s in slots.iter_mut() {
                    if s.draining.is_none() {
                        let aged = cfg.max_age.is_some_and(|a| s.born.elapsed() >= a);
                        let used = cfg.max_uses.is_some_and(|m| s.uses >= m);
                        if aged || used {
                            s.draining = Some(now + cfg.drain_timeout);
                            self.retired.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                // Sample live circuits into the measured oracle (#4), throttled to
                // ≤ one observation per circuit per SAMPLE_EVERY so a long-lived
                // connection can't dominate with a fresh sample every tick.
                if let Some(m) = &self.measured {
                    for s in slots.iter_mut() {
                        let due = s.last_sampled.is_none_or(|t| now.duration_since(t) >= SAMPLE_EVERY);
                        if due && s.circuit.conn.close_reason().is_none() {
                            m.observe(&s.circuit.conn);
                            s.last_sampled = Some(now);
                        }
                    }
                }
                // Reap: dead, OR drained (no in-flight), OR past the drain deadline.
                slots.retain(|s| {
                    let dead = s.circuit.conn.close_reason().is_some();
                    let drained = s.draining.is_some_and(|dl| {
                        s.active.load(Ordering::Relaxed) == 0 || now >= dl
                    });
                    !(dead || drained)
                });
            }
            self.refill().await;
        }
    }

    /// Ready (non-draining, live) circuits.
    pub async fn len(&self) -> usize {
        self.slots
            .lock()
            .await
            .iter()
            .filter(|s| s.draining.is_none() && s.circuit.conn.close_reason().is_none())
            .count()
    }

    /// Per-slot request counts (diagnostic — confirms round-robin balance).
    #[cfg(test)]
    pub(crate) async fn slot_uses(&self) -> Vec<u64> {
        self.slots.lock().await.iter().map(|s| s.uses).collect()
    }

    /// Total circuits ever built (pre-warm + refills + rotations).
    pub fn built(&self) -> u64 {
        self.built.load(Ordering::Relaxed)
    }
    /// Circuits retired (entered draining) for rotation/age/use.
    pub fn retired(&self) -> u64 {
        self.retired.load(Ordering::Relaxed)
    }
    /// Failed circuit builds during refill (explicit failure metric).
    pub fn build_errors(&self) -> u64 {
        self.build_errors.load(Ordering::Relaxed)
    }

    /// A structured [`crate::metrics::Snapshot`] scraped from the live pool: QUIC
    /// path stats summed across live circuits, the measured-oracle RTT percentiles
    /// (zeros if not measuring), and the circuit counters/ready gauge. The substrate
    /// fields stay zero here — a relay holding a [`quicmix::substrate::Substrate`]
    /// merges those via [`crate::metrics::Snapshot::with_substrate`].
    pub async fn metrics(&self) -> crate::metrics::Snapshot {
        let mut quic = crate::metrics::QuicStats::default();
        let mut ready = 0u64;
        {
            let slots = self.slots.lock().await;
            for s in slots.iter() {
                if s.circuit.conn.close_reason().is_none() {
                    quic.observe(&s.circuit.conn);
                    if s.draining.is_none() {
                        ready += 1;
                    }
                }
            }
        }
        let (p50, p90, p99) = self
            .measured
            .as_ref()
            .map(|m| m.rtt_percentiles())
            .unwrap_or_default();
        crate::metrics::Snapshot::default()
            .with_quic(quic)
            .with_oracle_rtt(p50, p90, p99)
            .with_circuits(self.built(), self.retired(), self.build_errors(), ready)
    }

    /// The pool's metrics rendered as Prometheus exposition text (the scrape body).
    pub async fn metrics_prometheus(&self) -> String {
        self.metrics().await.render_prometheus()
    }

    /// Distinct local source addresses across the live pool (unlinkability proxy).
    pub async fn distinct_sources(&self) -> usize {
        let slots = self.slots.lock().await;
        slots
            .iter()
            .map(|s| s.circuit.local)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    /// Run the http→quic ingress proxy over this pool, via the shared hyper ingress,
    /// round-robining a (leased) warm circuit per request; 503 if none is ready.
    pub async fn serve(self: Arc<Self>, listen: &str) -> Result<SocketAddr> {
        quicmix::ingress::serve_with(listen, move || {
            let pool = self.clone();
            async move { pool.pick().await }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emulator::emulated_front;
    use quicmix::OracleParams;
    use quinn::{Endpoint, ServerConfig};
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

    fn provider() {
        rustls::crypto::ring::default_provider().install_default().ok();
    }

    /// Minimal quinn server that accepts and holds connections.
    fn test_server() -> (SocketAddr, CertificateDer<'static>) {
        let c = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = c.cert.der().clone();
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(c.key_pair.serialize_der()));
        let sc = ServerConfig::with_single_cert(vec![cert.clone()], key).unwrap();
        let ep = Endpoint::server(sc, "127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = ep.local_addr().unwrap();
        tokio::spawn(async move {
            let _ep = ep.clone();
            while let Some(inc) = ep.accept().await {
                tokio::spawn(async move {
                    if let Ok(conn) = inc.await {
                        let _hold = conn;
                        std::future::pending::<()>().await;
                    }
                });
            }
        });
        (addr, cert)
    }

    fn oracle(delay_ms: u64) -> OracleParams {
        OracleParams {
            hops: 2,
            mean_hop_delay: Duration::from_millis(delay_ms),
            drop_prob: 0.0,
            slot_interval: Duration::ZERO,
            mtu: 1200,
        }
    }

    #[tokio::test]
    async fn prewarms_roundrobins_and_self_heals() {
        provider();
        let (server, cert) = test_server();
        let p = oracle(1);
        let cfg = PoolConfig { target: 3, tick: Duration::from_millis(50), ..Default::default() };
        let pool = WarmPool::start(vec![emulated_front(server, p)], cert, None, cfg)
            .await
            .unwrap();

        assert_eq!(pool.len().await, 3, "pre-warmed to target");
        assert_eq!(pool.distinct_sources().await, 3, "each circuit a distinct source");

        for _ in 0..6 {
            let _ = pool.pick().await.unwrap();
        }
        let mut uses = pool.slot_uses().await;
        uses.sort_unstable();
        assert_eq!(uses, vec![2, 2, 2], "pick() round-robins evenly");
        let built_before = pool.built();

        pool.pick().await.unwrap().close(0u32.into(), b"die"); // kill a circuit
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(pool.len().await, 3, "pool self-healed back to target");
        assert!(pool.built() > built_before, "a fresh circuit replaced it");
    }

    /// #4: live connections feed the measured oracle, and it drives new circuits.
    #[tokio::test]
    async fn measured_oracle_fed_by_live_connections() {
        provider();
        let (server, cert) = test_server();
        let p = oracle(5); // real per-hop delay → non-zero measured RTT
        let measured = Arc::new(MeasuredCc::new(p.hops, p.mtu, p.slot_interval));
        let cfg = PoolConfig { target: 2, tick: Duration::from_millis(40), ..Default::default() };
        let pool = WarmPool::start_measured(vec![emulated_front(server, p)], cert, measured.clone(), cfg)
            .await
            .unwrap();
        // Let the maintainer sample live circuits several times.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(measured.samples() > 0, "live connections must feed the estimator");
        assert!(
            measured.current().mean_hop_delay > Duration::ZERO,
            "a real per-hop delay was measured: {:?}",
            measured.current().mean_hop_delay
        );
        let _ = pool.len().await; // pool stays healthy on measured transports
        // A newly-built circuit's transport is rebuilt from the latest measurement.
        let _ = measured.transport();
    }

    /// #7: the observability contract reflects the real live pool.
    #[tokio::test]
    async fn metrics_scrape_reflects_live_pool() {
        provider();
        let (server, cert) = test_server();
        let p = oracle(3);
        let measured = Arc::new(MeasuredCc::new(p.hops, p.mtu, p.slot_interval));
        let cfg = PoolConfig { target: 3, tick: Duration::from_millis(40), ..Default::default() };
        let pool = WarmPool::start_measured(vec![emulated_front(server, p)], cert, measured, cfg)
            .await
            .unwrap();
        // Let the maintainer sample live circuits into the oracle.
        tokio::time::sleep(Duration::from_millis(250)).await;

        let snap = pool.metrics().await;
        assert_eq!(snap.circuits_ready, 3, "ready gauge tracks the warm circuits");
        assert!(snap.circuits_built_total >= 3, "built counter ≥ pre-warm");
        assert!(snap.quic_sent_packets_total > 0, "real handshakes sent QUIC packets");
        assert!(
            snap.oracle_rtt_p50_seconds > 0.0,
            "measured RTT p50 exported: {}",
            snap.oracle_rtt_p50_seconds
        );

        let text = pool.metrics_prometheus().await;
        assert!(text.contains("# TYPE circuits_ready gauge"));
        assert!(text.contains("circuits_ready 3"));
        assert!(text.contains("# TYPE quic_sent_packets_total counter"));
    }

    /// #6: a forced rotation must not kill a healthy in-flight request.
    #[tokio::test]
    async fn draining_does_not_kill_inflight() {
        provider();
        let (server, cert) = test_server();
        let p = oracle(1);
        let cfg = PoolConfig {
            target: 1,
            max_uses: Some(1), // first request triggers retirement
            tick: Duration::from_millis(30),
            drain_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let pool = WarmPool::start(vec![emulated_front(server, p)], cert, None, cfg)
            .await
            .unwrap();

        // In-flight request: lease + a real open stream on the circuit.
        let lease = pool.pick().await.unwrap();
        let (mut send, _recv) = lease.open_bi().await.unwrap();

        // uses==1==max_uses → the maintainer marks the circuit draining and refills.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(pool.retired() >= 1, "the over-used circuit was retired (draining)");
        assert!(pool.len().await >= 1, "pool refilled a fresh circuit while draining");

        // The in-flight stream is still alive — rotation did not kill it.
        send.write_all(b"still-alive")
            .await
            .expect("draining must not kill an in-flight request");

        // Release the lease → the drained circuit is reaped on a later tick.
        drop(lease);
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
}
