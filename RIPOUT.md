# STATUS — what's real, what's sim, what still needs verifying

(Formerly the rip-out list. The empty `nym.rs`/`katzenpost.rs` specs are **deleted**;
real bindings now live in integration crates. Nothing fake remains in the core.)

## Real bindings (separate crates) — now RUN LIVE (see `REAL_RESULTS.md`)
| crate | substrate | status |
|---|---|---|
| `quicmix-nym/` | Nym datagram mixnet | **real + VERIFIED LIVE on mainnet**: `MixTransport` over `nym-sdk`, wired end-to-end (`spawn_client_bridge` + `NymGateway`). Real QUIC + quicmix CC carried an HTTP fetch over live Nym (`bin/nym_e2e`); CC A/B (`bin/nym_bench`); unlinkable rotation verified (`bin/nym_rotate`, via `nym_front` + `rotation::connect_fresh_with`). |
| `quicmix-tor/` | Tor (stream) | **real + run live**: `MixTransport` over arti; a real circuit through `quicmix::tor::StreamSubstrate` reached check.torproject.org (HTTP 301). Datagram CC is a documented no-op on a stream (slow leg). |
| `quicmix-katzenpost/` | Katzenpost | **real CBOR + full data path VERIFIED LIVE** against `kpclientd`: handshake (`bin/kp_probe`) + PKI-resolved `SendMessage`→echo→`MessageReplyEvent` round-trip through the mixnet (`bin/kp_echo`). Schema mirrors Go `client/thin` + `core/pki`; `destination_id_hash = blake2b256(IdentityKey)`. Network validated (docker testnet, 5/5 ping). |

## Real & tested (core `quicmix`)
Transport mechanics, all unit-tested: `MixTransport` (object-safe), `striped`
(round-robin + merge), `rotation` (pool), `sched` + `client` (oracle-fed CC,
jitter-derived loss threshold), `oracle` (estimator), `tor::StreamDatagram`
(framing), `directory` (gateway dir + auto-promotion gate), the `node` proxy
(ingress+gateway, end-to-end over the emulator).

## Simulation — legitimate, but must be labeled (never "real network")
- `emulator.rs` (`EmulatedMixnet`) + `relay.rs`: in-process delay/drop/reorder/rate
  **simulation, no anonymity**. Used by unit tests and the demo bins
  (`bench`, `multipath`, `quicmix`, `rotate`). Present its numbers as **emulator**
  results, not real-network results.

## Status of the former gaps (most now CLOSED — see `REAL_RESULTS.md`)
1. ✅ **Live-network verification** of `quicmix-nym` / `quicmix-tor` — done on a
   laptop. Nym ran end-to-end over mainnet; Tor opened a real circuit.
2. ✅ **Katzenpost CBOR schema + full data path** — implemented and **verified live**
   against `kpclientd`: handshake + PKI-resolved `SendMessage`→echo→reply round-trip.
3. ✅ **Gateway-side** Nym loop — built as `quicmix_nym::NymGateway` (receive over
   Nym → local quinn server → reply via `send_reply`/SURB), verified by `nym_e2e`.
4. **Gossip transport** for `directory` (propagation/signing) — still TODO.
5. **Rate-weighted** multipath scheduler (replace naive round-robin) — still TODO.
6. **Rotation cost over real Nym** is an honest *negative* (warm pool ≈ cold; per-
   request mix latency dominates). Unlinkability holds; the cost win does not.
