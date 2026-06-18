# quicmix — roadmap

Forward work that builds on quicmix's existing seams (`MixTransport`, `Striped`,
`RotationPolicy` / `WarmPool`) and preserves its design invariants. Each item is a
self-contained, testable layer derived from a problem quicmix's own live runs
surfaced — worst case a no-op, best case it closes a gap the real-network numbers
exposed. None of them change what the client fundamentally is.

## Design invariants (every extension preserves these)

1. **Constant cell rate is an anonymity property**, not a throughput knob. Never send
   *more* to recover loss — the steady rate is what holds the anonymity set together.
2. **A fixed congestion window means no feedback loop.** `sched::OracleController`
   returns a constant window by design; a live-reactive window reintroduces the
   oscillation quicmix exists to avoid. Adapt *between* circuits (re-derive at the
   rotation boundary), never inside a live connection.
3. **QUIC's ARQ is the safety net.** Every layer sits above it and degrades to it.

## 1. In-budget forward error correction — highest value

**Problem (surfaced live):** on a real mixnet the round-trip is *seconds* (Nym p50 ≈
2.8 s), so a single dropped datagram recovered by ARQ costs a full multi-second
round-trip — the dominant reason the live goodput was muted. And invariant 1 forbids
sending extra cells to compensate.

**The idea:** carry erasure-coded redundancy *within* the existing constant-rate cell
budget. Send `k` payload + `m` parity cells at the rate the substrate already paces; a
dropped cell is reconstructed locally — **no extra rate, no extra round-trip.** This is
the right shape for a constant-rate, high-latency anonymity channel specifically: the
expensive operation is retransmission, and the bandwidth is fixed, so you spend a known
slice of a fixed budget to erase the round-trip cost of loss.

**Seam:** a `MixTransport` that wraps another `MixTransport` (composes exactly like
`striped::Striped`); the QUIC stack, congestion controller, and rotation above — and the
substrate below — are all unchanged.

**Durable implementation:**
- Systematic erasure code over fixed-size **generations** (Reed–Solomon, or XOR parity
  for small `m`); systematic ⇒ zero decode cost when nothing is lost.
- Size `m` from the oracle's `drop_prob` (`m ≈ ceil(k·p/(1−p)) + margin`); self-tuning,
  no manual knob.
- Bounded memory: one fixed generation window, evicted on completion or timeout.
- Graceful degradation: loss beyond `m` in a generation falls through to QUIC's ARQ —
  strictly never worse than today.
- Test: in-process over `EmulatedMixnet` with injected `drop_prob`; assert recovery with
  no ARQ round-trip and an unchanged send rate.

## 2. Rate-weighted multipath

**Problem:** `striped::Striped` round-robins datagrams equally, so the slowest substrate
head-of-line-blocks the tail when paths differ in speed.

**The idea:** weight the split by each substrate's measured rate (`mtu / slot_interval`,
already on its `OracleParams`) with smooth weighted round-robin; unknown rate → equal
weight; a slow or dead path collapses to ~0 weight on its own. Contained to `striped.rs`,
no architecture change.

## 3. Entry-leg indistinguishability

**Problem:** the mixnet hides the *path*, but the client→first-hop leg can still reveal
*that a mixnet client is running* — a metadata signal the substrate itself doesn't cover.

**The idea:** shape the entry leg so it isn't fingerprintable as a quicmix client
(fixed-size cells — already true on the datagram substrates — plus generic transport
framing on the entry hop). Optional, behind a flag, lower priority than 1–2.

## 4. Rotation defaults

`RotationPolicy` + `WarmPool` already implement self-healing, off-hot-path rotation (the
maintainer reaps aged / over-used / dead circuits and refills). The remaining work is to
ship sane defaults — rotate on age/use, on by default — so the unlinkability the rest of
the design enables is a property you get without asking.

## Out of scope (would change what the client fundamentally is)

- **Live-adaptive CC window** — breaks invariant 2. Re-derive per circuit at the rotation
  boundary instead.
- **Sending extra to beat loss** — breaks invariant 1. Recover within the budget (item 1).
- **Replacing QUIC's ARQ** — it stays as the safety net under FEC.

## The through-line

Every item is a layer on a seam that already exists, sized from values quicmix already
measures, with a graceful fallback to today's behavior. That's what makes them durable:
they extend the client without changing what it is.
