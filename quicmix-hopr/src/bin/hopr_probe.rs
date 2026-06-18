//! Live HOPR probe — run on a host with a hoprd node to VERIFY the binding.
//!
//! Usage:
//!   cargo run --manifest-path quicmix/quicmix-hopr/Cargo.toml --bin hopr_probe -- \
//!     http://127.0.0.1:3001 <X_AUTH_TOKEN> <DEST_PEER_ID> [hops]
//!
//! Sends a small message to DEST and pops it back (point DEST at the node's own
//! peer id for a loopback round-trip), printing the round-trip result. This is the
//! verification step the sandbox could not run.

use quicmix_hopr::{hopr_oracle, HoprSubstrate};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 4 {
        eprintln!("usage: hopr_probe <api_url> <token> <dest_peer_id> [hops]");
        std::process::exit(2);
    }
    let (api, token, dest) = (a[1].clone(), a[2].clone(), a[3].clone());
    let hops: u8 = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(1);

    // Rate-capped, substrate-appropriate oracle (the CC's timing model for HOPR).
    let oracle = hopr_oracle(hops);
    let hopr = HoprSubstrate::connect(api, token, dest, hops, 4242, oracle);

    let payload = b"quicmix-hopr-probe";
    println!("sending {} bytes via hoprd…", payload.len());
    let t = Instant::now();
    hopr.send_one(payload).await?;

    // poll pop for up to 30s
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(got) = hopr.pop_one().await? {
            println!(
                "ROUND-TRIP OK in {:.0} ms: {:?}",
                t.elapsed().as_secs_f64() * 1e3,
                String::from_utf8_lossy(&got)
            );
            return Ok(());
        }
        if Instant::now() > deadline {
            anyhow::bail!("no message popped within 30s (check dest peer id / hops / connectivity)");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
