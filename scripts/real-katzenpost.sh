#!/usr/bin/env bash
# Real test: Katzenpost. HONEST STATUS — there is no public Katzenpost mainnet
# like Nym's, and the reference client is Go, so there is NO Rust probe to run
# and NO quicmix Katzenpost binding yet. A "real test" requires standing up
# Katzenpost's docker testnet and using THEIR Go tooling. This script brings up
# that testnet so you can validate the network; the quicmix binding is TODO.
#
# Prereqs: git, docker, docker compose, Go (for their ping client).
set -euo pipefail

echo "[real-katzenpost] NOTE: no public mainnet; this uses Katzenpost's docker testnet."
echo "[real-katzenpost] There is no quicmix Katzenpost binding yet — this only"
echo "                  validates that the Katzenpost network itself works."

WORK="${KATZENPOST_DIR:-/tmp/katzenpost}"
if [ ! -d "$WORK" ]; then
  git clone https://github.com/katzenpost/katzenpost "$WORK"
fi
cd "$WORK"
echo "[real-katzenpost] bringing up the docker testnet (see docker/ in the repo)…"
echo "  cd $WORK/docker && make up        # start the local mixnet"
echo "  cd $WORK/docker && make ping      # their Go client pings through it"
echo
echo "Follow $WORK/docker/README.rst for exact targets on your platform."
echo "quicmix integration: implement quicmix/src/katzenpost.rs against their"
echo "client (socket/SOCKS), then add it to striped::Striped like Nym."
