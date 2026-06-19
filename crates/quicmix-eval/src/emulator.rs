//! A local mixnet *emulator*: a one-way datagram pipe that imposes the four
//! dynamics a real mixnet inflicts on traffic — per-hop random delay, drops,
//! reordering, and (via the oracle) a constant-rate slot budget.
//!
//! It exposes its **exact** parameters through [`MixTransport::oracle`], so the
//! Area-A scheduler can be developed against ground truth before facing an
//! *estimated* oracle on a real mixnet. Parameters are meant to be tuned to
//! mimic measured Nym characteristics.

use quicmix::{MixTransport, OracleParams};
use rand::Rng;
use std::collections::VecDeque;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, sleep, Duration};

/// An emulated mix path. `send` injects a datagram; surviving datagrams are
/// delayed (sum of `hops` i.i.d. exponentials), reordered (independent per-packet
/// delays), then queued into a **finite** egress buffer drained at the
/// constant-rate slot budget (`slot_interval`).
///
/// The finite buffer is the important realism: when offered load exceeds the
/// rate, the buffer fills and **tail-drops** — so loss is *load-dependent* and
/// spikes exactly when a sender overdrives the bottleneck, just like a real mix.
/// (An earlier version had an unbounded queue + load-independent loss, which
/// unfairly rewarded an "ignore loss + huge window" strategy.) `slot_interval ==
/// 0` disables pacing/queueing (no bandwidth cap; for delay/drop/reorder tests).
pub struct EmulatedMixnet {
    params: OracleParams,
    in_tx: mpsc::UnboundedSender<Vec<u8>>,
    out_rx: Mutex<mpsc::UnboundedReceiver<Vec<u8>>>,
}

impl EmulatedMixnet {
    /// Unbounded egress buffer (no overload tail-drop); only the per-packet
    /// `drop_prob` causes loss. Use for delay/drop/reorder tests.
    pub fn new(params: OracleParams) -> Self {
        Self::with_queue(params, usize::MAX)
    }

    /// `max_queue` bounds the egress buffer (in packets); offered load above the
    /// slot rate fills it and tail-drops — load-dependent loss.
    pub fn with_queue(params: OracleParams, max_queue: usize) -> Self {
        let (in_tx, mut in_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (ready_tx, mut ready_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (out_tx, out_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        // Stage 1: per-datagram base drop + random delay (produces reordering).
        tokio::spawn(async move {
            while let Some(dg) = in_rx.recv().await {
                let (drop, delay) = {
                    let mut rng = rand::thread_rng();
                    let drop = rng.gen::<f64>() < params.drop_prob;
                    // Sum of `hops` i.i.d. exponential delays (mean = mean_hop_delay):
                    // X = -mean * ln(U), U ~ Uniform(0,1].
                    let mut secs = 0.0;
                    for _ in 0..params.hops {
                        let u: f64 = rng.gen::<f64>().max(1e-12);
                        secs += -params.mean_hop_delay.as_secs_f64() * u.ln();
                    }
                    (drop, Duration::from_secs_f64(secs))
                };
                if drop {
                    continue;
                }
                let ready = ready_tx.clone();
                tokio::spawn(async move {
                    sleep(delay).await;
                    let _ = ready.send(dg);
                });
            }
        });

        // Stage 2: finite egress buffer drained one cell per `slot_interval`.
        tokio::spawn(async move {
            if params.slot_interval.is_zero() {
                // No rate cap: pass through (preserves delay-induced reorder).
                while let Some(dg) = ready_rx.recv().await {
                    let _ = out_tx.send(dg);
                }
                return;
            }
            let mut q: VecDeque<Vec<u8>> = VecDeque::new();
            let mut ticker = interval(params.slot_interval);
            let mut closed = false;
            loop {
                tokio::select! {
                    maybe = ready_rx.recv(), if !closed => match maybe {
                        Some(dg) => {
                            if q.len() < max_queue {
                                q.push_back(dg); // else: tail-drop (overload loss)
                            }
                        }
                        None => closed = true,
                    },
                    _ = ticker.tick() => {
                        if let Some(dg) = q.pop_front() {
                            let _ = out_tx.send(dg);
                        } else if closed {
                            break;
                        }
                    }
                }
            }
        });

        Self {
            params,
            in_tx,
            out_rx: Mutex::new(out_rx),
        }
    }
}

#[async_trait::async_trait]
impl MixTransport for EmulatedMixnet {
    fn oracle(&self) -> OracleParams {
        self.params
    }

    async fn send(&self, datagram: Vec<u8>) {
        let _ = self.in_tx.send(datagram);
    }

    async fn recv(&self) -> Option<Vec<u8>> {
        self.out_rx.lock().await.recv().await
    }
}

// --- Emulator-backed fronts for `quicmix::rotation` -------------------------
//
// These drive the production rotation seam ([`quicmix::rotation`]) over the
// in-process emulator. They are the eval counterparts of a real substrate's
// front factory (e.g. `quicmix-nym`'s `nym_front`).

use anyhow::Result;
use quicmix::rotation::{connect_fresh_with, Circuit, CircuitPool, FrontFactory};
use rustls::pki_types::CertificateDer;
use std::net::SocketAddr;
use std::sync::Arc;

/// Emulator [`FrontFactory`]: each call stands up a fresh in-process mix relay to
/// `server_addr` (two `EmulatedMixnet` legs), the eval analogue of a real
/// substrate front.
pub fn emulated_front(server_addr: SocketAddr, p: OracleParams) -> FrontFactory {
    Arc::new(move || {
        Box::pin(async move {
            quicmix::relay::start_relay(
                server_addr,
                Arc::new(EmulatedMixnet::new(p)),
                Arc::new(EmulatedMixnet::new(p)),
            )
            .await
            .map(|r| r.front)
            .map_err(anyhow::Error::from)
        })
    })
}

/// Build one fresh, unlinkable circuit over the emulator. Thin wrapper over
/// [`connect_fresh_with`] + [`emulated_front`] (quinn-default transport — the
/// emulator's RTT is milliseconds).
pub async fn connect_fresh(
    server_addr: SocketAddr,
    server_cert: CertificateDer<'static>,
    p: OracleParams,
) -> Result<Circuit> {
    connect_fresh_with(&emulated_front(server_addr, p), server_cert, None).await
}

/// Pre-warm `n` circuits over the emulator. Thin wrapper over
/// [`CircuitPool::prewarm_with`] + [`emulated_front`].
pub async fn prewarm(
    n: usize,
    server_addr: SocketAddr,
    server_cert: CertificateDer<'static>,
    p: OracleParams,
) -> Result<CircuitPool> {
    CircuitPool::prewarm_with(n, &emulated_front(server_addr, p), server_cert, None).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(drop_prob: f64) -> OracleParams {
        OracleParams {
            hops: 3,
            mean_hop_delay: Duration::from_millis(5),
            drop_prob,
            slot_interval: Duration::ZERO, // pacing off for delay/drop/reorder tests
            mtu: 1200,
        }
    }

    #[tokio::test]
    async fn delivers_all_when_no_drops_and_reorders() {
        let mix = EmulatedMixnet::new(params(0.0));
        let n = 300u32;
        for i in 0..n {
            mix.send(i.to_le_bytes().to_vec()).await;
        }
        let mut got = Vec::new();
        while got.len() < n as usize {
            match tokio::time::timeout(Duration::from_secs(3), mix.recv()).await {
                Ok(Some(dg)) => got.push(u32::from_le_bytes(dg.try_into().unwrap())),
                _ => break,
            }
        }
        assert_eq!(got.len(), n as usize, "all datagrams delivered when drop_prob=0");
        // Independent per-hop delays should reorder: received order is not sorted.
        let sorted = got.windows(2).all(|w| w[0] <= w[1]);
        assert!(!sorted, "expected reordering from independent per-hop delays");
    }

    #[tokio::test]
    async fn drops_reduce_delivery() {
        let mix = EmulatedMixnet::new(params(0.8));
        let n = 400u32;
        for i in 0..n {
            mix.send(i.to_le_bytes().to_vec()).await;
        }
        let mut count = 0usize;
        loop {
            match tokio::time::timeout(Duration::from_millis(800), mix.recv()).await {
                Ok(Some(_)) => count += 1,
                _ => break,
            }
        }
        assert!(count < n as usize, "drops should reduce delivery below the sent count");
        assert!(count > 0, "not everything should drop at p=0.8");
    }

    #[tokio::test]
    async fn oracle_reports_exact_params() {
        let p = params(0.1);
        let mix = EmulatedMixnet::new(p);
        assert_eq!(mix.oracle(), p);
    }

    #[tokio::test]
    async fn slot_interval_paces_egress() {
        // 30 packets at one-per-10ms cannot all arrive faster than ~290ms.
        let p = OracleParams {
            hops: 1,
            mean_hop_delay: Duration::from_millis(1),
            drop_prob: 0.0,
            slot_interval: Duration::from_millis(10),
            mtu: 1200,
        };
        let mix = EmulatedMixnet::new(p);
        let n = 30usize;
        for i in 0..n {
            mix.send((i as u32).to_le_bytes().to_vec()).await;
        }
        let start = tokio::time::Instant::now();
        for _ in 0..n {
            let _ = mix.recv().await.unwrap();
        }
        // (n-1) gaps of 10ms is a hard lower bound the pacer cannot beat.
        assert!(start.elapsed() >= Duration::from_millis((n as u64 - 1) * 10));
    }
}
