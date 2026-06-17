#!/usr/bin/env bash
# Run the real-network probes that ARE implemented (Nym, Tor). Each measures the
# live substrate. Katzenpost is separate (no public net) — see real-katzenpost.sh.
set -uo pipefail
here="$(dirname "$0")"
echo "=== Nym (live mainnet) ==="; "$here/real-nym.sh" "${1:-20}"      || echo "[nym] FAILED (see output)"
echo; echo "=== Tor (live, via arti) ==="; "$here/real-tor.sh"          || echo "[tor] FAILED (see output)"
echo; echo "Katzenpost: run scripts/real-katzenpost.sh (docker testnet; no public mainnet)."
