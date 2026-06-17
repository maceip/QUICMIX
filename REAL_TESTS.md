# Real-network tests (run on a laptop with open egress)

These hit **live** networks. They could not complete in the dev sandbox (egress
filtered to HTTPS/443 only); they should run on a normal laptop.

> Honest scope: these scripts **measure the real substrates** (RTT / loss /
> bootstrap). They are *not* a full quicmix-over-real-network data path — the Nym
> and Katzenpost `MixTransport` bindings are not implemented yet (see RIPOUT.md).
> Use the measurements to (a) prove the real networks are reachable and (b)
> calibrate the emulator's `OracleParams` to reality.

## Nym (live mainnet) — implemented, real
```sh
quicmix/scripts/real-nym.sh 30
```
Connects an ephemeral `nym-sdk` client to mainnet, sends 30 self-addressed pings,
prints returned/sent, median RTT, loss%. Only needs cargo + open egress.

## Tor (live, via arti) — implemented, real
```sh
quicmix/scripts/real-tor.sh 1.1.1.1:80
```
Bootstraps real Tor (embedded arti — no system `tor` needed), opens a circuit
through quicmix's `StreamSubstrate`, measures bootstrap + connect + first-byte
RTT. If arti errors on state-dir permissions, uncomment the
`FS_MISTRUST_DISABLE_PERMISSIONS_CHECKS=1` line in the script.

## Katzenpost — NOT a one-command real test
No public mainnet; reference client is Go. `scripts/real-katzenpost.sh` clones
their repo and points you at their **docker testnet** + Go `ping` to validate the
network. There is **no quicmix Katzenpost binding yet**.

## Both probes at once
```sh
quicmix/scripts/run-all-real.sh
```

## First build note
The first run compiles a large dependency tree (`nym-sdk` / `arti-client`) — a
few minutes, once. Both crates are isolated so the core `quicmix` build never
pulls them.
