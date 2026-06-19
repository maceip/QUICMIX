//! quicmix-eval — the simulation/evaluation harness for `quicmix`.
//!
//! This crate is **not** part of the shipping client. The production `quicmix`
//! binary never touches any of these modules; they exist purely to exercise and
//! measure the production crate's contributions over an in-process mixnet
//! **emulator**:
//!
//!   * [`emulator`] — a local datagram pipe that imposes mixnet delay/drop/reorder,
//!     plus the emulator-backed [`emulator::emulated_front`]/[`emulator::connect_fresh`]/
//!     [`emulator::prewarm`] helpers that drive `quicmix::rotation` over it.
//!   * [`striped`] — multipath striping across several substrates.
//!   * [`proxy`] — the `WarmPool` rotation/multipath proxy used by the harnesses.
//!   * [`metrics`] — Prometheus exposition for the harness.
//!
//! The production substrate seam (`quicmix::substrate::Substrate`,
//! `quicmix::relay`, `quicmix::rotation`) lives in the `quicmix` crate; real
//! substrates plug into it. The runnable benchmarks/self-tests live in `examples/`.

pub mod emulator;
pub mod metrics;
pub mod proxy;
pub mod striped;
