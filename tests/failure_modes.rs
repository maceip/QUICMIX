//! #9 — failure-mode matrix. Each real failure must produce a **typed error**, a
//! **metric**, and **bounded recovery** (no hang, no unbounded growth, heals when
//! the fault clears). HOPR's HTTP-status failures (401/500/timeout/dead-node) are
//! covered by real mock-server tests in the `quicmix-hopr` crate; this file covers
//! the substrate boundary and the warm-circuit pool:
//!
//! - nym disconnect / katz socket close → inner substrate returns `Closed`
//! - gateway dies with an active pool → in-flight survives + `build_errors` + heal
//! - queue saturation → typed `Backpressure` + `dropped` metric + bounded queue
//! - oracle change under concurrent load → safe, bounded, watch reflects final
//! - prewarm partial failure → `build_errors` + bounded heal to target

use quicmix::client::MeasuredCc;
use quicmix::proxy::{PoolConfig, WarmPool};
use quicmix::rotation::{emulated_front, FrontFactory};
use quicmix::substrate::Substrate;
use quicmix::{MixTransport, OracleParams, SubstrateError};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use quinn::{Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

fn provider() {
    rustls::crypto::ring::default_provider().install_default().ok();
}

fn tiny_oracle() -> OracleParams {
    OracleParams {
        hops: 1,
        mean_hop_delay: Duration::from_millis(1),
        drop_prob: 0.0,
        slot_interval: Duration::ZERO, // drain the pacer immediately
        mtu: 1200,
    }
}

/// A substrate that delivers `ok_sends` datagrams then "disconnects": every later
/// `try_send` and *every* `try_recv` returns `Closed` (models a nym client dropping
/// its SURB context, or a katzenpost daemon socket closing).
struct Faulty {
    sent: AtomicU64,
    ok_sends: u64,
}
#[async_trait::async_trait]
impl MixTransport for Faulty {
    fn oracle(&self) -> OracleParams {
        tiny_oracle()
    }
    async fn send(&self, _: Vec<u8>) {}
    async fn recv(&self) -> Option<Vec<u8>> {
        None
    }
    async fn try_send(&self, _: Vec<u8>) -> Result<(), SubstrateError> {
        if self.sent.fetch_add(1, Ordering::Relaxed) < self.ok_sends {
            Ok(())
        } else {
            Err(SubstrateError::Closed)
        }
    }
    async fn try_recv(&self) -> Result<Vec<u8>, SubstrateError> {
        Err(SubstrateError::Closed)
    }
}

#[tokio::test]
async fn nym_disconnect_send_is_typed_and_counted_not_dropped() {
    // First 5 sends succeed, then the substrate disconnects.
    let inner: Arc<dyn MixTransport> = Arc::new(Faulty { sent: AtomicU64::new(0), ok_sends: 5 });
    let s = Substrate::new(inner, 64);
    for i in 0..40u32 {
        s.send(i.to_le_bytes().to_vec()).await.unwrap(); // enqueue
    }
    // Let the pump drain the bounded queue.
    let mut m = s.metrics();
    for _ in 0..200 {
        m = s.metrics();
        if m.sent + m.send_errors >= 40 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(m.sent, 5, "only the pre-disconnect sends succeed");
    assert!(m.send_errors >= 1, "post-disconnect sends are counted, not silently dropped");
    assert!(m.sent + m.send_errors <= 41, "bounded: no phantom sends");
}

#[tokio::test]
async fn katz_socket_close_recv_surfaces_typed_error_bounded() {
    let inner: Arc<dyn MixTransport> = Arc::new(Faulty { sent: AtomicU64::new(0), ok_sends: 0 });
    let s = Substrate::new(inner, 8);
    // recv must surface the typed Closed promptly, never hang.
    let got = tokio::time::timeout(Duration::from_secs(2), s.recv()).await;
    match got {
        Ok(Err(SubstrateError::Closed)) => {}
        other => panic!("expected typed Closed within bound, got {other:?}"),
    }
}

#[tokio::test]
async fn queue_saturation_is_typed_backpressure_with_dropped_metric() {
    // Slow pacer + tiny queue: a flood must surface Backpressure (typed) on the
    // non-blocking path, count drops on the lossy path, and stay bounded.
    let slow = OracleParams { slot_interval: Duration::from_millis(80), ..tiny_oracle() };
    struct Pacer(OracleParams);
    #[async_trait::async_trait]
    impl MixTransport for Pacer {
        fn oracle(&self) -> OracleParams {
            self.0
        }
        async fn send(&self, _: Vec<u8>) {}
        async fn recv(&self) -> Option<Vec<u8>> {
            None
        }
    }
    let s = Substrate::new(Arc::new(Pacer(slow)), 4);
    // Non-blocking path: must report typed Backpressure once the queue fills.
    let mut backpressure = false;
    for i in 0..500u32 {
        if s.try_send(i.to_le_bytes().to_vec()) == Err(SubstrateError::Backpressure) {
            backpressure = true;
            break;
        }
    }
    assert!(backpressure, "saturation must surface typed Backpressure");
    // Lossy path: floods are dropped + counted, never buffered without bound.
    for i in 0..2000u32 {
        MixTransport::send(&*s, i.to_le_bytes().to_vec()).await;
    }
    let m = s.metrics();
    assert!(m.queue_depth <= 4, "queue bounded by capacity, got {}", m.queue_depth);
    assert!(m.dropped > 0, "overload is dropped + visible");
}

#[tokio::test]
async fn oracle_change_under_concurrent_load_is_safe_and_bounded() {
    let s = Substrate::new(Arc::new(StubPacer), 256);
    let mut w = s.oracle_watch();
    // Concurrent senders.
    let mut tasks = Vec::new();
    for t in 0..8u32 {
        let s = s.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..200u32 {
                let _ = s.try_send((t * 1000 + i).to_le_bytes().to_vec());
                if i % 32 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }));
    }
    // Hammer oracle updates while sends are in flight.
    for slot_ms in [1u64, 7, 3, 11, 5, 9] {
        s.update_oracle(OracleParams {
            slot_interval: Duration::from_millis(slot_ms),
            ..tiny_oracle()
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Everything completes within a bound (no deadlock from concurrent updates).
    for t in tasks {
        tokio::time::timeout(Duration::from_secs(5), t)
            .await
            .expect("senders must finish — oracle churn must not deadlock")
            .unwrap();
    }
    // The watch reflects the final published oracle.
    let _ = w.changed().await;
    assert_eq!(s.oracle().slot_interval, Duration::from_millis(9));
}

struct StubPacer;
#[async_trait::async_trait]
impl MixTransport for StubPacer {
    fn oracle(&self) -> OracleParams {
        tiny_oracle()
    }
    async fn send(&self, _: Vec<u8>) {}
    async fn recv(&self) -> Option<Vec<u8>> {
        None
    }
}

// ---- circuit-pool failures ----

fn test_server() -> (std::net::SocketAddr, CertificateDer<'static>) {
    let c = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = c.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(c.key_pair.serialize_der()));
    let sc = ServerConfig::with_single_cert(vec![cert.clone()], key).unwrap();
    let ep = Endpoint::server(sc, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = ep.local_addr().unwrap();
    tokio::spawn(async move {
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

/// A front factory that works while `up` is true and returns an error (gateway
/// unreachable) while it is false — built atop the real emulated front.
fn flippable_front(
    server: std::net::SocketAddr,
    p: OracleParams,
    up: Arc<AtomicBool>,
) -> FrontFactory {
    let working = emulated_front(server, p);
    Arc::new(move || {
        let up = up.clone();
        let working = working.clone();
        Box::pin(async move {
            if up.load(Ordering::Relaxed) {
                working().await
            } else {
                Err(anyhow::anyhow!("gateway down"))
            }
        })
    })
}

#[tokio::test]
async fn gateway_dies_with_active_pool_keeps_inflight_and_heals() {
    provider();
    let (server, cert) = test_server();
    let p = tiny_oracle();
    let up = Arc::new(AtomicBool::new(true));
    let cfg = PoolConfig {
        target: 2,
        max_uses: Some(1), // a served request retires its circuit → forces a refill
        tick: Duration::from_millis(30),
        drain_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let pool = WarmPool::start(vec![flippable_front(server, p, up.clone())], cert, None, cfg)
        .await
        .unwrap();
    assert_eq!(pool.len().await, 2, "pre-warmed");

    // An in-flight request on a leased circuit, with a real open stream.
    let lease = pool.pick().await.unwrap();
    let (mut send, _r) = lease.open_bi().await.unwrap();

    // The gateway dies: new circuit builds now fail.
    up.store(false, Ordering::Relaxed);

    // The lease+max_uses forces retirement → refill attempts fail → build_errors.
    let mut errs = 0;
    for _ in 0..100 {
        errs = pool.build_errors();
        if errs >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(errs >= 1, "gateway death is counted as build_errors, got {errs}");

    // The in-flight request is NOT killed by the failures around it.
    send.write_all(b"survives-gateway-death")
        .await
        .expect("active in-flight request must survive a gateway outage");

    // Recovery is bounded: when the gateway returns, the pool heals back to target.
    up.store(true, Ordering::Relaxed);
    drop(lease);
    let mut healed = 0;
    for _ in 0..150 {
        healed = pool.len().await;
        if healed >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(healed, 2, "pool heals to target once the gateway recovers");
}

#[tokio::test]
async fn prewarm_partial_failure_counts_and_heals() {
    provider();
    let (server, cert) = test_server();
    let p = tiny_oracle();
    let working = emulated_front(server, p);
    // Fail the first 3 build attempts, then succeed — a flaky bootstrap.
    let calls = Arc::new(AtomicU64::new(0));
    let front: FrontFactory = {
        let calls = calls.clone();
        Arc::new(move || {
            let calls = calls.clone();
            let working = working.clone();
            Box::pin(async move {
                if calls.fetch_add(1, Ordering::Relaxed) < 3 {
                    Err(anyhow::anyhow!("bootstrap not ready"))
                } else {
                    working().await
                }
            })
        })
    };
    let cfg = PoolConfig { target: 2, tick: Duration::from_millis(25), ..Default::default() };
    let measured = Arc::new(MeasuredCc::new(p.hops, p.mtu, p.slot_interval));
    let pool = WarmPool::start_measured(vec![front], cert, measured, cfg).await.unwrap();

    // Bounded recovery: despite the early failures, the pool reaches target and the
    // failures are visible in build_errors.
    let mut healed = 0;
    for _ in 0..200 {
        healed = pool.len().await;
        if healed >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(healed, 2, "pool heals to target after a flaky bootstrap");
    assert!(pool.build_errors() >= 1, "early bootstrap failures are counted");
}
