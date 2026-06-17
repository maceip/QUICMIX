//! Round-robin striping across multiple substrates — multipath over the mix.
//!
//! `Striped` is itself a [`MixTransport`] that wraps N underlying substrates
//! (e.g. Nym + Katzenpost + a Tor adapter), **round-robins datagrams** across
//! them on send, and **merges** their receive streams. The QUIC layer above sees
//! one pipe; reassembly is QUIC's own (packet numbers + stream offsets), which is
//! why the reorder/loss tolerance in `client.rs` matters — the paths have
//! different latency, so cross-path reordering is large.
//!
//! ⚠️ Anonymity trade-off: striping one flow over several networks creates a
//! cross-network correlation surface (matching timing/volume on both). Against a
//! global/coalition observer this is a *throughput-for-anonymity* trade — opt-in,
//! not the default. See PLUGGABILITY.md.
//!
//! Note: naive round-robin sends equal shares regardless of path speed, so the
//! **slowest path head-of-line-blocks** the tail. A rate-weighted scheduler (send
//! proportional to each oracle's BDP) is the obvious next step; the demo shows
//! the naive behaviour so the effect is visible.

use crate::{MixTransport, OracleParams};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

pub struct Striped {
    subs: Vec<Arc<dyn MixTransport>>,
    next: AtomicUsize,
    merged: Mutex<mpsc::UnboundedReceiver<Vec<u8>>>,
    oracle: OracleParams,
}

impl Striped {
    pub fn new(subs: Vec<Arc<dyn MixTransport>>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        for s in &subs {
            let (s, tx) = (s.clone(), tx.clone());
            tokio::spawn(async move {
                while let Some(dg) = s.recv().await {
                    let _ = tx.send(dg);
                }
            });
        }
        let oracle = blend(&subs);
        Self {
            subs,
            next: AtomicUsize::new(0),
            merged: Mutex::new(rx),
            oracle,
        }
    }
}

/// A blended oracle for the combined path: rate ≈ sum of per-substrate rates;
/// latency ≈ the slowest path (it bounds the tail). Used to size the QUIC
/// window/timers over the striped pipe.
fn blend(subs: &[Arc<dyn MixTransport>]) -> OracleParams {
    if subs.is_empty() {
        return OracleParams {
            hops: 1,
            mean_hop_delay: Duration::ZERO,
            drop_prob: 0.0,
            slot_interval: Duration::ZERO,
            mtu: 1200,
        };
    }
    let mtu = subs[0].oracle().mtu;
    let mut rate = 0.0f64;
    let mut max_path = Duration::ZERO;
    let mut drop_sum = 0.0;
    for s in subs {
        let p = s.oracle();
        rate += if p.slot_interval.is_zero() {
            1e9
        } else {
            p.mtu as f64 / p.slot_interval.as_secs_f64()
        };
        max_path = max_path.max(p.mean_hop_delay * p.hops);
        drop_sum += p.drop_prob;
    }
    OracleParams {
        hops: 1,
        mean_hop_delay: max_path,
        drop_prob: drop_sum / subs.len() as f64,
        slot_interval: if rate > 0.0 {
            Duration::from_secs_f64(mtu as f64 / rate)
        } else {
            Duration::ZERO
        },
        mtu,
    }
}

#[async_trait::async_trait]
impl MixTransport for Striped {
    fn oracle(&self) -> OracleParams {
        self.oracle
    }

    async fn send(&self, datagram: Vec<u8>) {
        if self.subs.is_empty() {
            return;
        }
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.subs.len();
        self.subs[i].send(datagram).await;
    }

    async fn recv(&self) -> Option<Vec<u8>> {
        self.merged.lock().await.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emulator::EmulatedMixnet;

    fn p(slot_ms: u64, hops: u32, delay_ms: u64) -> OracleParams {
        OracleParams {
            hops,
            mean_hop_delay: Duration::from_millis(delay_ms),
            drop_prob: 0.0,
            slot_interval: Duration::from_millis(slot_ms),
            mtu: 1200,
        }
    }

    #[tokio::test]
    async fn round_robins_and_merges_across_substrates() {
        let a: Arc<dyn MixTransport> = Arc::new(EmulatedMixnet::new(p(0, 1, 1)));
        let b: Arc<dyn MixTransport> = Arc::new(EmulatedMixnet::new(p(0, 1, 1)));
        let striped = Striped::new(vec![a, b]);
        let n = 200usize;
        for i in 0..n {
            striped.send((i as u32).to_le_bytes().to_vec()).await;
        }
        let mut got = 0;
        while got < n {
            match tokio::time::timeout(Duration::from_secs(2), striped.recv()).await {
                Ok(Some(_)) => got += 1,
                _ => break,
            }
        }
        assert_eq!(got, n, "all datagrams delivered across both substrates");
    }
}
