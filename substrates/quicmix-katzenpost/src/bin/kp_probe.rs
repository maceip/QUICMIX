//! kp_probe — verify the **real Katzenpost thin-client CBOR handshake** against a
//! live `kpclientd` daemon (T5).
//!
//! Brings the quicmix-katzenpost binding from a pass-through stub to verified real
//! interop: it speaks the actual length-prefixed CBOR protocol to the daemon and
//! completes the connection handshake.
//!
//! Prereq: the Katzenpost docker testnet is up (`docker/ make start`), exposing
//! `kpclientd` on `127.0.0.1:64331`.
//!
//! Run: `cargo run -p quicmix-katzenpost --bin kp_probe -- [host:port]`

use anyhow::Result;
use quicmix_katzenpost::connect_and_handshake;

#[tokio::main]
async fn main() -> Result<()> {
    let daemon = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:64331".to_string());
    eprintln!("connecting to Katzenpost thin-client daemon at {daemon}…");
    let (_stream, hs) = connect_and_handshake(&daemon).await?;
    println!("Katzenpost thin-client handshake OK  [real CBOR vs live kpclientd]:");
    println!("  daemon connected to mixnet: {}", hs.is_connected);
    println!("  PKI document delivered:     {} bytes", hs.pki_doc.len());
    println!("  session resumed:            {}", hs.session_resumed);
    println!("\n✅ the quicmix-katzenpost binding speaks the real thin-client CBOR protocol.");
    Ok(())
}
