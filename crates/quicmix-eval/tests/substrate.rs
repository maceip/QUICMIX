//! Substrate-boundary behaviour (`quicmix::substrate::Substrate`), exercised over
//! the in-process emulator. These moved out of the production crate when the
//! emulator did: the seam is production, the emulator that drives it is eval.

use quicmix::substrate::Substrate;
use quicmix::{MixTransport, OracleParams, SubstrateError};
use quicmix_eval::emulator::EmulatedMixnet;
use std::sync::Arc;
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
