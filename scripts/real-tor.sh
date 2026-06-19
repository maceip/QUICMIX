#!/usr/bin/env bash
# Real test: open a circuit on the LIVE Tor network via arti and measure it
# (bootstrap time, stream connect, first-byte RTT) through quicmix's
# StreamSubstrate trait. Needs open egress. MEASURES the real substrate; it is
# not a full quicmix-over-Tor data path.
set -euo pipefail
cd "$(dirname "$0")/.."            # -> quicmix/
TARGET="${1:-1.1.1.1:80}"
# arti needs a writable HOME for its state dir:
export HOME="${HOME:-/tmp}"
# Only needed on hosts with odd dir ownership (sandboxes); harmless to leave on
# a laptop, but commented so the laptop run keeps arti's permission check:
# export FS_MISTRUST_DISABLE_PERMISSIONS_CHECKS=1
echo "[real-tor] building + probing real Tor circuit to ${TARGET}…"
exec cargo run --release --manifest-path substrates/quicmix-tor/Cargo.toml --bin tor_probe -- "$TARGET"
