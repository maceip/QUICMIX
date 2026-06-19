//! realprobe — measure the *real* Nym mainnet mixnet, and emit `OracleParams`.
//!
//! Connects an ephemeral Nym client to mainnet, sends N self-addressed pings of a
//! fixed size, and measures round-trip latency, loss, and end-to-end throughput.
//! It then prints a ready-to-use `quicmix::OracleParams` derived from those
//! measurements — the input the quicmix scheduler consumes on a real substrate
//! (and the calibration target for the emulator). This is **T1**: turn a live
//! substrate into real CC parameters.
//!
//! Methodology: sends are **decoupled** from receives. We fire all N pings (each
//! tagged with a unique id, carrying a payload of MTU bytes), record each send
//! time, then drain replies for a global collection window, matching by id.
//!  - RTT  = per-ping (reply_arrival - its_send_time); reported p50/p90/max.
//!  - loss = pings whose reply never arrived within the window.
//!  - throughput = (received-1) / (last_arrival - first_arrival) * MTU bytes/s,
//!    i.e. the sustained end-to-end reply rate through the mix (both directions).
//! A tight per-ping timeout would mistake the multi-second mainnet latency tail
//! (Poisson per-hop mix delay, forward + SURB return) for loss — hence the window.
//!
//! Usage: `realprobe [N] [WINDOW_SECS] [MTU_BYTES] [HOPS]`
//!        defaults: 30 pings, 120s window, 1200B payload, 3 hops.

use anyhow::Result;
use futures::StreamExt;
use nym_sdk::mixnet::{IncludedSurbs, MixnetMessageSender};
use std::collections::HashMap;
use std::time::{Duration, Instant};

fn arg<T: std::str::FromStr>(n: usize, default: T) -> T {
    std::env::args().nth(n).and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<()> {
    let n: usize = arg(1, 30);
    let window = Duration::from_secs(arg(2, 120));
    let mtu: usize = arg(3, 1200);
    let hops: u32 = arg(4, 3);

    eprintln!("connecting to Nym mainnet (ephemeral client, force_tls=wss/443)…");
    let mut client = nym_sdk::mixnet::MixnetClientBuilder::new_ephemeral()
        .force_tls(true)
        .build()?
        .connect_to_mixnet()
        .await?;
    let me = client.nym_address().to_string();
    eprintln!("connected as {me}");
    eprintln!("sending {n} self-addressed pings ({mtu}B each), collecting up to {window:?}…");

    // ---- Fire all pings up front, each tagged "ping-<i>" + padding to MTU.
    let mut sent_at: HashMap<String, Instant> = HashMap::new();
    let dest = *client.nym_address();
    for i in 0..n {
        let tag = format!("ping-{i}");
        let mut body = tag.clone().into_bytes();
        body.resize(mtu, 0); // pad to MTU so throughput reflects real packet size
        sent_at.insert(tag, Instant::now());
        // Self-addressed with reply-SURBs (the path a gateway would use to reply).
        client.send_message(dest, body, IncludedSurbs::new(2)).await?;
    }

    // ---- Drain replies for the global window, matching by tag.
    let mut rtts: Vec<Duration> = Vec::new();
    let mut arrivals: Vec<Instant> = Vec::new();
    let start = Instant::now();
    while rtts.len() < n {
        let remaining = match window.checked_sub(start.elapsed()) {
            Some(r) if !r.is_zero() => r,
            _ => break,
        };
        match tokio::time::timeout(remaining, client.next()).await {
            Ok(Some(msg)) => {
                // Tag is the leading "ping-<i>" prefix of the padded payload.
                let lead = &msg.message[..msg.message.len().min(16)];
                if let Ok(s) = std::str::from_utf8(lead) {
                    if let Some(tag) = s.split('\0').next() {
                        if let Some(t) = sent_at.remove(tag) {
                            rtts.push(t.elapsed());
                            arrivals.push(Instant::now());
                        }
                    }
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    client.disconnect().await;

    // ---- Stats.
    let received = rtts.len();
    rtts.sort();
    let pct = |p: f64| -> Duration {
        if rtts.is_empty() {
            return Duration::ZERO;
        }
        let idx = ((rtts.len() as f64 - 1.0) * p).round() as usize;
        rtts[idx]
    };
    let p50 = pct(0.50);
    let p90 = pct(0.90);
    let max = pct(1.0);
    let loss = 1.0 - (received as f64 / n as f64);

    // Throughput from reply-arrival span (sustained end-to-end rate through mix).
    arrivals.sort();
    let (thru_msgs_s, thru_bytes_s) = if arrivals.len() >= 2 {
        let span = (*arrivals.last().unwrap() - arrivals[0]).as_secs_f64().max(1e-6);
        let r = (arrivals.len() - 1) as f64 / span;
        (r, r * mtu as f64)
    } else {
        (0.0, 0.0)
    };

    println!("\nreal Nym mainnet: {received}/{n} returned (window {window:?})");
    println!("  RTT p50: {:.0} ms", p50.as_secs_f64() * 1e3);
    println!("  RTT p90: {:.0} ms", p90.as_secs_f64() * 1e3);
    println!("  RTT max: {:.0} ms", max.as_secs_f64() * 1e3);
    println!("  loss:    {:.1}%", loss * 100.0);
    println!("  throughput: {:.1} msg/s ({:.1} KB/s @ {mtu}B)", thru_msgs_s, thru_bytes_s / 1024.0);

    // ---- Derived OracleParams (T1 output): the CC parameters for this substrate.
    let mean_hop_delay = p50 / (2 * hops.max(1));
    let slot_interval = if thru_msgs_s > 0.0 {
        Duration::from_secs_f64(1.0 / thru_msgs_s)
    } else {
        Duration::ZERO
    };
    println!("\n// measured OracleParams for the Nym substrate (paste into the gateway/client):");
    println!("OracleParams {{");
    println!("    hops: {hops},");
    println!("    mean_hop_delay: Duration::from_millis({}),", mean_hop_delay.as_millis());
    println!("    drop_prob: {:.3},", loss);
    println!("    slot_interval: Duration::from_millis({}),", slot_interval.as_millis());
    println!("    mtu: {mtu},");
    println!("}}");
    Ok(())
}
