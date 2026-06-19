#!/usr/bin/env bash
# Real test: probe the LIVE Nym mainnet mixnet (RTT + loss).
# Needs open egress (it connects to a Nym gateway). Prints returned/sent,
# median RTT, loss%. This MEASURES the real substrate — it is not a full
# quicmix-over-Nym data path (that binding is not implemented yet).
set -euo pipefail
cd "$(dirname "$0")/.."            # -> quicmix/
N="${1:-30}"                        # number of self-addressed pings
echo "[real-nym] building + probing live Nym mainnet (${N} pings)…"
exec cargo run --release --manifest-path substrates/quicmix-nym/Cargo.toml --bin nym_probe -- "$N"
