//! kp_echo — a **full Katzenpost data-path round-trip** through the live testnet.
//!
//! Resolves the `echo` service from the PKI document the daemon delivers, sends a
//! real `SendMessage` (with a reply SURB) carrying a recognizable nonce, and waits
//! for the `MessageReplyEvent` — verifying the payload comes back. This is the
//! piece beyond the handshake: actual application data over the Katzenpost mixnet,
//! via the quicmix-katzenpost binding.
//!
//! Prereq: the docker testnet is up (`kpclientd` on `127.0.0.1:64331`, with a PKI
//! consensus that includes an `echo` service node).
//!
//! Run: `cargo run -p quicmix-katzenpost --bin kp_echo -- [host:port] [capability]`

use anyhow::Result;
use quicmix_katzenpost::request_reply;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let daemon = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:64331".to_string());
    let capability = std::env::args().nth(2).unwrap_or_else(|| "echo".to_string());

    // A recognizable 32-byte nonce to find in the echoed reply.
    let nonce: Vec<u8> = (0..32u8).map(|i| i.wrapping_mul(37).wrapping_add(11)).collect();

    eprintln!("connecting to kpclientd at {daemon}; resolving '{capability}' from the PKI doc…");
    let (service, reply) =
        request_reply(&daemon, &capability, nonce.clone(), Duration::from_secs(90)).await?;

    let dest_hex: String = service
        .destination_id_hash
        .iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect();
    println!(
        "resolved '{capability}': queue={:?}  dest_id_hash={dest_hex}…  (blake2b256 of node IdentityKey)",
        String::from_utf8_lossy(&service.recipient_queue_id)
    );
    println!(
        "SendMessage → MessageReplyEvent: error_code={} payload={} bytes",
        reply.error_code,
        reply.payload.len()
    );

    let echoed = reply.error_code == 0
        && reply
            .payload
            .windows(nonce.len())
            .any(|w| w == nonce.as_slice());
    if echoed {
        println!(
            "\n✅ full round-trip over the live Katzenpost mixnet: the echo service returned our payload."
        );
        Ok(())
    } else {
        anyhow::bail!(
            "reply did not contain our payload (error_code={}, {} bytes back)",
            reply.error_code,
            reply.payload.len()
        );
    }
}
