# quicmix E2E test matrix

The authoritative list of end-to-end commands, each with its **prerequisites**,
**timeout bound** (kill it if it exceeds this — something is wrong), **what to look
for**, and **real captured output** from an actual run. Two tiers:

- **Tier 1 — deterministic, local, no network.** Reproducible anywhere `cargo`
  builds. Captured output below is from a clean run on macOS (darwin 25.5),
  2026-06-18. Numbers (timings/goodput) vary run-to-run; the *shape* is stable.
- **Tier 2 — live networks.** Hit real Nym / Tor / Katzenpost / HOPR. Need open
  egress and (Katzenpost/HOPR) a reachable node. Captured output is the dated,
  attributed record from `REAL_RESULTS.md` (open-egress laptop, 2026-06-16).

Run Tier 1 in CI; run Tier 2 on a laptop with egress when validating a substrate.

---

## Tier 1 — deterministic local E2E

### T1.1 — full workspace test suite

```sh
cargo test --workspace
```

- **Prereqs:** cargo only.
- **Timeout bound:** 120 s (first build longer; the run itself is < 5 s).
- **Look for:** every `test result:` line says `ok`, `0 failed`. Includes the unit
  tests, the `failure_modes` integration suite (#9), and the substrate bindings'
  own tests.
- **Captured:**

```
Running unittests src/lib.rs
test result: ok. 28 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
Running tests/failure_modes.rs
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

The HOPR failure-mapping tests live in their **isolated** crate (the substrate
bindings are standalone, not workspace members, so `--workspace` skips them — test
each with `--manifest-path`):

```sh
cargo test --manifest-path quicmix-hopr/Cargo.toml
```

```
running 5 tests
test tests::cc_builds_from_hopr_oracle_rate_capped ... ok
test tests::dead_node_maps_to_closed ... ok
test tests::http_500_maps_to_remote_rejected ... ok
test tests::http_401_maps_to_auth_failed ... ok
test tests::wedged_node_times_out_bounded ... ok
test result: ok. 5 passed; 0 failed
```

### T1.2 — pooled multipath proxy + observability scrape (flagship)

```sh
cargo run --bin proxy
```

- **Prereqs:** cargo only (everything is in-process: origin, gateway, mix emulator).
- **Timeout bound:** 30 s.
- **Look for:** `N/N concurrent requests OK`; the pool self-heals back to `target`
  after rotation (`built total` > target ⇒ rotations happened); a full Prometheus
  metrics block (#7) with real `quic_sent_packets_total` and `circuits_*` values.
  `substrate_*` and `oracle_rtt_*` read `0` here **honestly** — this bin uses a
  fixed transport (`WarmPool::start`), not the online-measured path
  (`start_measured`), and the substrate counters live on the relay side.
- **Captured:**

```
pooled multipath proxy:
  transport:     oracle-fed CC (Congestion::Quicmix, BDP+tolerant timers)
  substrate:     Striped round-robin (2 emulated mixnets per circuit)
  pre-warmed:    4 circuits ready, 4 distinct sources
  proxy:         http://127.0.0.1:59014  →  gateway → origin 127.0.0.1:59013

24/24 concurrent requests OK in 79 ms
after rotation settles: 4 circuits ready (target 4), 8 built total = 4 rotations

--- metrics (prometheus exposition) ---
# TYPE substrate_sent_total counter
substrate_sent_total 0
# TYPE quic_sent_packets_total counter
quic_sent_packets_total 57
# TYPE quic_lost_packets_total counter
quic_lost_packets_total 0
# TYPE circuits_built_total counter
circuits_built_total 8
# TYPE circuits_retired_total counter
circuits_retired_total 4
# TYPE circuits_ready gauge
circuits_ready 4
```

### T1.3 — unlinkable rotation + pre-warm speedup

```sh
cargo run --bin rotate
```

- **Prereqs:** cargo only.
- **Timeout bound:** 60 s.
- **Look for:** a warm pool pre-warms; warm rotation is faster than cold (emulator
  speedup ≈ 3×); one session id carried across **2 circuits** with **2 distinct
  source addresses** and no shared keys/resumption ticket. (The warm speedup is an
  *emulator* property — `REAL_RESULTS.md` T4 records that it does **not** hold on
  real Nym, where multi-second mix RTT dominates. Unlinkability holds on both.)
- **Captured:**

```
pre-warmed pool: 11 circuits ready
rotation cost (median of 11, establish-if-needed + first round-trip):
  cold (build fresh circuit):   160.7 ms  (handshake + data RTT)
  warm (take from pool):         51.0 ms  (data RTT only)
  speedup: 3.1x
session continuity / unlinkability:
  session id (e2e, inside tunnel): 45c54d0b983a1cc5
  carried over 2 circuits; 2 distinct apparent source addrs: [127.0.0.1:51669, 127.0.0.1:59680]
  => same logical session, no shared source addr / keys / resumption ticket. OK
```

### T1.4 — congestion-control A/B (the core claim, on the emulator)

```sh
cargo run --bin bench
```

- **Prereqs:** cargo only.
- **Timeout bound:** 120 s (the `cubic` arm intentionally runs to a 50 s FCT cap —
  that *is* the result: stock CUBIC collapses over a lossy reordering mix).
- **Look for:** `quicmix` beats `cubic` and `cubic+timers` on FCT/goodput with
  fewer spurious retransmits, at the emulator's 2% drop.
- **Captured:**

```
scenario: 4 MB | hops=3 mean_hop_delay=5ms drop=0.02 rate_cap≈1.20 MB/s
cc                fct(s)  goodput(MB/s)  lost/sent     loss%
cubic             50.000           0.08   770/4369     17.6%
cubic+timers      11.650           0.36    87/3055      2.8%  (4.3x)
quicmix            7.178           0.58    81/3067      2.6%  (7.0x)
```

### T1.5 — multipath striping (honest negative)

```sh
cargo run --bin multipath
```

- **Prereqs:** cargo only.
- **Timeout bound:** 30 s.
- **Look for:** round-robin across a fast + slow substrate; goodput sits **between**
  the two (naive equal-share striping is HOL-bound by the slow path — documented,
  with the rate-weighted fix named).
- **Captured:**

```
scenario: 2 MB | fast≈2.4 MB/s (2×3ms)  slow≈0.6 MB/s (5×15ms)
path                   goodput(MB/s)
fast substrate alone         0.90
slow substrate alone         0.38
round-robin (both)           0.72
```

---

## Tier 2 — live-network E2E

These need open egress; Katzenpost/HOPR additionally need a reachable node. The
captured output is the dated record in `REAL_RESULTS.md` (2026-06-16). First build
of a substrate crate compiles a large tree (`nym-sdk` / `arti-client`) — minutes,
once; the core `quicmix` build never pulls them (isolated crates).

### T2.1 — Nym mainnet: measured oracle + QUIC/CC end-to-end + rotation

```sh
# isolated crate → run by manifest path (or `cd quicmix-nym && cargo run --bin ...`)
cargo run --manifest-path quicmix-nym/Cargo.toml --bin nym_e2e    # QUIC + quicmix CC over live Nym
cargo run --manifest-path quicmix-nym/Cargo.toml --bin nym_bench  # CC A/B (cubic vs quicmix)
cargo run --manifest-path quicmix-nym/Cargo.toml --bin nym_rotate # unlinkable rotation over Nym
```

- **Prereqs:** open egress to Nym mainnet gateways. No account/funds.
- **Timeout bound:** 5 min per bin (multi-second mix RTT; bootstrap dominates).
- **Look for:** a QUIC connection *established over Nym*; an HTTP fetch returned
  through the mix; quicmix CC produces **0** spurious retransmits where stock CUBIC
  produces ≥1; rotation carries one session over two fresh Nym identities.
- **Captured (REAL_RESULTS.md T1/T2/T4):**

```
realprobe: 30/30 returned (0% loss)  RTT p50 2823 ms  p90 4598 ms  6.3 msg/s
→ OracleParams { hops: 3, mean_hop_delay: 470ms, drop_prob: 0.0, slot_interval: 158ms, mtu: 1200 }

nym_e2e: QUIC connection established over Nym in 2.5 s
         end-to-end (A→Nym→B): OK (80 bytes, 6.6 s)
         origin response: "hello from origin via real Nym + quicmix"

nym_bench:  cubic   FCT 2.1s  7.64 KB/s  retransmits 1/25  loss 4.0%
            quicmix FCT 1.8s  8.75 KB/s  retransmits 0/24  loss 0.0%

nym_rotate: one e2e session id over 2 circuits; 2 distinct source addrs  => OK
            (cost: warm pre-warm does NOT beat cold on real Nym — honest negative)
```

### T2.2 — Tor (arti, embedded): real circuit + probe

```sh
quicmix/scripts/real-tor.sh 1.1.1.1:80
```

- **Prereqs:** open egress; embedded arti (no system `tor` needed). If arti errors
  on state-dir permissions, set `FS_MISTRUST_DISABLE_PERMISSIONS_CHECKS=1`.
- **Timeout bound:** 5 min (cold bootstrap is slow).
- **Look for:** a real bootstrap + circuit + first-byte RTT; a confirmed Tor exit.
  Tor is a **stream** substrate — quicmix's datagram CC is a documented no-op there
  (the "slow leg"); the binding still runs a real circuit and measures it.
- **Captured (REAL_RESULTS.md T1):**

```
torprobe: bootstrap 160 s   stream connect 13.4 s   first-byte RTT 677 ms
→ check.torproject.org returned HTTP/1.1 301  (confirms a real Tor exit)
```

### T2.3 — Katzenpost (docker voting testnet): CBOR data path

```sh
cargo run --manifest-path quicmix-katzenpost/Cargo.toml --bin kp_probe  # connect + receive live PKI
cargo run --manifest-path quicmix-katzenpost/Cargo.toml --bin kp_echo   # SendMessage → +echo → reply
```

- **Prereqs:** a running `kpclientd` (no public mainnet — bring up Katzenpost's
  docker voting testnet, 17 containers). Point the bins at the daemon socket.
- **Timeout bound:** 2 min once the testnet is up.
- **Look for:** the daemon delivers a real PKI document; an echo round-trip through
  the mix completes (real CBOR thin-client schema, not a stub).
- **Captured (REAL_RESULTS.md T5):**

```
kp_probe: daemon connected; 27394-byte PKI doc delivered
kp_echo:  parsed PKI; resolved +echo@servicenode1; SendMessage → echo → reply OK
network:  PKI consensus signed by 3 dirauths; Go ping 5/5 Sphinx packets, 100%
```

### T2.4 — HOPR (hoprd REST): failure mapping verified; live data path pending

```sh
cargo run --manifest-path quicmix-hopr/Cargo.toml --bin hopr_probe   # needs a funded hoprd node
```

- **Prereqs:** a running hoprd node (a funded one, or a local `pluto` dev cluster)
  with its REST API token. **Not yet exercised against a live node.**
- **Timeout bound:** 2 min.
- **What IS verified now (T1.1, this matrix):** the real HTTP error mapping against
  a raw-TCP mock hoprd — 401/403→`AuthFailed`, 5xx→`RemoteRejected`, wedged
  node→`Timeout` (bounded), dead node→`Closed`. The wire protocol is validated
  against hoprd's generated SDK (REST v3.1.0). The **live data path** still needs a
  node — run `hopr_probe` there to confirm end-to-end.
