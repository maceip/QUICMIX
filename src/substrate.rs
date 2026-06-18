//! First-class substrate boundary: observability + control over any [`MixTransport`].
//!
//! The raw [`MixTransport`] trait is intentionally tiny (`send`/`recv`/`oracle`), which
//! makes it easy to implement but blind at the boundary: `send` swallows errors,
//! `recv`'s `None` conflates EOF with failure, there's no backpressure, and `oracle`
//! is a frozen snapshot. [`Substrate`] is an **additive layer** — it wraps any
//! `MixTransport` and makes four things first-class, without changing a single
//! substrate impl:
//!
//! - **errors** — [`Substrate::send`]/[`Substrate::recv`] return a typed [`SubstrateError`].
//! - **pacing** — a send pump releases datagrams one per `slot_interval` (the
//!   constant-rate budget becomes *enforced and observable*, not buried per-impl).
//! - **backpressure** — a bounded queue: [`Substrate::send`] awaits room rather than
//!   silently dropping; [`Substrate::try_send`] reports [`SubstrateError::Backpressure`].
//! - **measured-oracle updates** — a `watch` channel of [`OracleParams`]; the pacer
//!   follows it live, and the CC reads it **at the rotation boundary** (never
//!   mid-connection — that's the no-feedback-loop invariant).
//!
//! Plus [`Metrics`] (sent/received/dropped/queue depth) for observability. `Substrate`
//! itself implements `MixTransport`, so it's a drop-in decorator that composes with
//! [`crate::striped::Striped`], [`crate::relay`], etc.

use crate::{MixTransport, OracleParams, SubstrateError, SubstrateKind};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, Mutex};

/// Observable boundary counters.
#[derive(Default, Debug)]
pub struct Metrics {
    pub sent: AtomicU64,
    pub received: AtomicU64,
    /// Real send failures from the inner substrate (mapped [`SubstrateError`]s).
    pub send_errors: AtomicU64,
    /// Datagrams dropped by the *lossy* `MixTransport::send` path under backpressure.
    pub dropped: AtomicU64,
    /// Current depth of the paced send queue.
    pub queue_depth: AtomicUsize,
    /// How long the most-recently-sent datagram waited in the queue (µs) — i.e. the
    /// age of the oldest queued item at send time. Rises under sustained overload.
    pub enqueue_latency_us: AtomicU64,
}

/// A point-in-time copy of [`Metrics`].
#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    pub sent: u64,
    pub received: u64,
    pub send_errors: u64,
    pub dropped: u64,
    pub queue_depth: usize,
    pub enqueue_latency_us: u64,
}

impl Metrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            sent: self.sent.load(Ordering::Relaxed),
            received: self.received.load(Ordering::Relaxed),
            send_errors: self.send_errors.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            enqueue_latency_us: self.enqueue_latency_us.load(Ordering::Relaxed),
        }
    }
}

/// The observable, controllable boundary around a substrate.
pub struct Substrate {
    send_tx: mpsc::Sender<(Instant, Vec<u8>)>,
    recv_rx: Mutex<mpsc::Receiver<Result<Vec<u8>, SubstrateError>>>,
    oracle_tx: watch::Sender<OracleParams>,
    oracle_rx: watch::Receiver<OracleParams>,
    kind: SubstrateKind,
    metrics: Arc<Metrics>,
}

impl Substrate {
    /// Wrap `inner` with a paced, bounded, observable boundary. `queue_depth` is the
    /// backpressure threshold (datagrams buffered ahead of the paced sender).
    pub fn new(inner: Arc<dyn MixTransport>, queue_depth: usize) -> Arc<Self> {
        let depth = queue_depth.max(1);
        let kind = inner.kind();
        let (oracle_tx, oracle_rx) = watch::channel(inner.oracle());
        let (send_tx, mut send_rx) = mpsc::channel::<(Instant, Vec<u8>)>(depth);
        let (recv_tx, recv_rx) = mpsc::channel::<Result<Vec<u8>, SubstrateError>>(depth);
        let metrics = Arc::new(Metrics::default());

        // Send pump: drain the queue, pace one datagram per (live) slot_interval.
        {
            let (inner, metrics, oracle_rx) = (inner.clone(), metrics.clone(), oracle_rx.clone());
            tokio::spawn(async move {
                while let Some((enqueued, dg)) = send_rx.recv().await {
                    metrics.queue_depth.store(send_rx.len(), Ordering::Relaxed);
                    // Age of the oldest-queued item (= how long this one waited).
                    metrics
                        .enqueue_latency_us
                        .store(enqueued.elapsed().as_micros() as u64, Ordering::Relaxed);
                    // Fallible inner send — real errors are counted, never dropped.
                    match inner.try_send(dg).await {
                        Ok(()) => {
                            metrics.sent.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            metrics.send_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    let slot = oracle_rx.borrow().slot_interval;
                    if !slot.is_zero() {
                        tokio::time::sleep(slot).await;
                    }
                }
            });
        }
        // Receive pump: forward inner datagrams; closing the channel signals EOF.
        {
            let (inner, metrics) = (inner.clone(), metrics.clone());
            tokio::spawn(async move {
                loop {
                    match inner.try_recv().await {
                        Ok(dg) => {
                            metrics.received.fetch_add(1, Ordering::Relaxed);
                            if recv_tx.send(Ok(dg)).await.is_err() {
                                break;
                            }
                        }
                        // Surface the typed error to the consumer, then end the pump.
                        Err(e) => {
                            let _ = recv_tx.send(Err(e)).await;
                            break;
                        }
                    }
                }
            });
        }

        Arc::new(Self {
            send_tx,
            recv_rx: Mutex::new(recv_rx),
            oracle_tx,
            oracle_rx,
            kind,
            metrics,
        })
    }

    /// Backpressured **enqueue**: awaits until the paced queue has room, then returns
    /// `Ok`. Important: `Ok` means the datagram was *queued*, **not** that the inner
    /// substrate sent it — the paced pump sends asynchronously, so a real inner send
    /// failure is reported via the [`Metrics::send_errors`] counter (and, for the
    /// receive direction, as a typed [`Substrate::recv`] error), not by this return
    /// value. `Err(Closed)` here means only that the pump/queue is gone (the
    /// `Substrate` was dropped) — i.e. nothing more can ever be sent.
    pub async fn send(&self, datagram: Vec<u8>) -> Result<(), SubstrateError> {
        self.send_tx
            .send((Instant::now(), datagram))
            .await
            .map_err(|_| SubstrateError::Closed)
    }

    /// Backpressured send with a deadline. Waits for queue room up to `timeout`; if
    /// the queue stays full past it (sustained overload), returns the **explicit**
    /// [`SubstrateError::Backpressure`] rather than blocking forever or dropping.
    pub async fn send_timeout(
        &self,
        datagram: Vec<u8>,
        timeout: Duration,
    ) -> Result<(), SubstrateError> {
        match tokio::time::timeout(timeout, self.send_tx.send((Instant::now(), datagram))).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(SubstrateError::Closed),
            Err(_) => Err(SubstrateError::Backpressure),
        }
    }

    /// Non-blocking send: `Err(Backpressure)` if the queue is full (the caller is
    /// outrunning the rate), `Err(Closed)` if the substrate is gone.
    pub fn try_send(&self, datagram: Vec<u8>) -> Result<(), SubstrateError> {
        self.send_tx.try_send((Instant::now(), datagram)).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => SubstrateError::Backpressure,
            mpsc::error::TrySendError::Closed(_) => SubstrateError::Closed,
        })
    }

    /// Receive a datagram, surfacing the substrate's typed error (e.g. `AuthFailed`,
    /// `RemoteRejected`) or `Closed` when it ends.
    pub async fn recv(&self) -> Result<Vec<u8>, SubstrateError> {
        self.recv_rx.lock().await.recv().await.unwrap_or(Err(SubstrateError::Closed))
    }

    /// A live, watchable view of the measured [`OracleParams`]. Consume it **at the
    /// rotation boundary** to re-derive the CC; the pacer here already follows it.
    pub fn oracle_watch(&self) -> watch::Receiver<OracleParams> {
        self.oracle_rx.clone()
    }

    /// Publish freshly measured params (e.g. from [`crate::oracle::OracleEstimator`]).
    /// Takes effect immediately for pacing; the CC picks it up at the next rotation.
    pub fn update_oracle(&self, params: OracleParams) {
        let _ = self.oracle_tx.send(params);
    }

    /// A snapshot of the boundary counters.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }
}

#[async_trait::async_trait]
impl MixTransport for Substrate {
    fn kind(&self) -> SubstrateKind {
        self.kind
    }
    fn oracle(&self) -> OracleParams {
        *self.oracle_rx.borrow()
    }
    /// Lossy drop-in send: under backpressure it drops and counts (the trait can't
    /// report errors — use [`Substrate::send`]/[`Substrate::try_send`] for those).
    async fn send(&self, datagram: Vec<u8>) {
        if self.send_tx.try_send((Instant::now(), datagram)).is_err() {
            self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    async fn recv(&self) -> Option<Vec<u8>> {
        match self.recv_rx.lock().await.recv().await {
            Some(Ok(dg)) => Some(dg),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emulator::EmulatedMixnet;
    use std::time::Duration;

    fn p(slot_ms: u64) -> OracleParams {
        OracleParams {
            hops: 1,
            mean_hop_delay: Duration::from_millis(1),
            drop_prob: 0.0,
            slot_interval: Duration::from_millis(slot_ms),
            mtu: 1200,
        }
    }

    #[tokio::test]
    async fn carries_datagrams_and_counts() {
        let emu: Arc<dyn MixTransport> = Arc::new(EmulatedMixnet::new(p(0)));
        let s = Substrate::new(emu, 64);
        for i in 0..20u32 {
            s.send(i.to_le_bytes().to_vec()).await.unwrap();
        }
        let mut got = 0;
        while got < 20 {
            match tokio::time::timeout(Duration::from_secs(2), s.recv()).await {
                Ok(Ok(_)) => got += 1,
                _ => break,
            }
        }
        assert_eq!(got, 20, "all datagrams delivered through the boundary");
        let m = s.metrics();
        assert_eq!(m.sent, 20);
        assert_eq!(m.received, 20);
        assert_eq!(m.dropped, 0);
    }

    #[tokio::test]
    async fn try_send_signals_backpressure() {
        // A slow pacer (100 ms/slot) + a tiny queue → the queue fills fast.
        let emu: Arc<dyn MixTransport> = Arc::new(EmulatedMixnet::new(p(100)));
        let s = Substrate::new(emu, 2);
        let mut hit_backpressure = false;
        for i in 0..200u32 {
            if s.try_send(i.to_le_bytes().to_vec()) == Err(SubstrateError::Backpressure) {
                hit_backpressure = true;
                break;
            }
        }
        assert!(hit_backpressure, "a full paced queue must surface backpressure");
    }

    #[tokio::test]
    async fn overload_is_bounded_and_visible() {
        // Slow pacer (50 ms/slot) + small queue → a flood cannot grow memory.
        let emu: Arc<dyn MixTransport> = Arc::new(EmulatedMixnet::new(p(50)));
        let s = Substrate::new(emu, 4);
        let flood = 2000u32;
        for i in 0..flood {
            // Lossy trait send: never blocks, drops when full → bounded memory.
            MixTransport::send(&*s, i.to_le_bytes().to_vec()).await;
        }
        let m = s.metrics();
        assert!(m.queue_depth <= 4, "queue bounded by capacity, got {}", m.queue_depth);
        assert!(m.dropped > 0, "overload beyond the queue is dropped + visible, not buffered");
        assert!(m.sent + m.dropped <= flood as u64 + 4, "no phantom traffic");
    }

    #[tokio::test]
    async fn send_timeout_returns_explicit_backpressure() {
        // Slow pacer (100 ms/slot) + a 1-slot queue: sustained sends past the
        // deadline must report Backpressure, not block forever or silently drop.
        let emu: Arc<dyn MixTransport> = Arc::new(EmulatedMixnet::new(p(100)));
        let s = Substrate::new(emu, 1);
        let mut got = false;
        for _ in 0..50 {
            if s.send_timeout(vec![0u8; 8], Duration::from_millis(20)).await
                == Err(SubstrateError::Backpressure)
            {
                got = true;
                break;
            }
        }
        assert!(got, "sustained overload past the deadline must return Backpressure");
    }

    /// A transport whose fallible ops fail, to prove errors are counted + surfaced.
    struct Failing;
    #[async_trait::async_trait]
    impl MixTransport for Failing {
        fn oracle(&self) -> OracleParams {
            p(0)
        }
        async fn send(&self, _: Vec<u8>) {}
        async fn recv(&self) -> Option<Vec<u8>> {
            None
        }
        async fn try_send(&self, _: Vec<u8>) -> Result<(), SubstrateError> {
            Err(SubstrateError::RemoteRejected)
        }
        async fn try_recv(&self) -> Result<Vec<u8>, SubstrateError> {
            Err(SubstrateError::AuthFailed)
        }
    }

    #[tokio::test]
    async fn inner_failures_are_counted_and_surfaced() {
        let inner: Arc<dyn MixTransport> = Arc::new(Failing);
        let s = Substrate::new(inner, 8);
        // a send that the inner rejects: counted as send_errors, not sent.
        s.send(b"x".to_vec()).await.unwrap(); // enqueue succeeds; pump's try_send fails
        for _ in 0..100 {
            if s.metrics().send_errors >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let m = s.metrics();
        assert!(m.send_errors >= 1, "inner send failure must increment send_errors");
        assert_eq!(m.sent, 0, "a failed send is not counted as sent");
        // recv surfaces the inner's typed error to the caller.
        let got = s.recv().await;
        assert!(
            matches!(got, Err(SubstrateError::AuthFailed) | Err(SubstrateError::Closed)),
            "recv must surface a typed error, got {got:?}"
        );
    }

    #[tokio::test]
    async fn oracle_updates_are_observable() {
        let emu: Arc<dyn MixTransport> = Arc::new(EmulatedMixnet::new(p(10)));
        let s = Substrate::new(emu, 8);
        let mut w = s.oracle_watch();
        assert_eq!(w.borrow().slot_interval, Duration::from_millis(10));
        s.update_oracle(p(42));
        w.changed().await.unwrap();
        assert_eq!(w.borrow().slot_interval, Duration::from_millis(42));
        assert_eq!(s.oracle().slot_interval, Duration::from_millis(42));
    }
}
