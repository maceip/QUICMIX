//! Area A — the oracle-driven controller.
//!
//! Verified against `quinn_proto::congestion::Controller`: a custom controller
//! returns a **constant `window()`** (sized from the oracle's slot budget /
//! bandwidth-delay product) and implements **`on_congestion_event()` as a
//! no-op** — i.e. *rate is exogenous, anonymity drops are not congestion*. This
//! is the minimal expression of the Area-A idea; richer scheduling (per-stream
//! slot allocation, oracle-seeded loss timers) builds on top.
//!
//! The anonymity-critical constant rate is enforced at the `MixTransport`
//! boundary, not here, so this controller only has to avoid letting QUIC's
//! stock congestion logic throttle/over-react to mix-induced loss & reordering.

use quinn::congestion::{Controller, ControllerFactory};
use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

/// A congestion controller with a fixed window that never reacts to loss.
#[derive(Debug, Clone)]
pub struct OracleController {
    window: u64,
}

impl Controller for OracleController {
    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        _lost_bytes: u64,
    ) {
        // Rate is exogenous and mix drops are not congestion: do nothing.
    }

    fn on_mtu_update(&mut self, _new_mtu: u16) {}

    fn window(&self) -> u64 {
        self.window
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.window
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Factory wiring [`OracleController`] into a `quinn::TransportConfig`.
#[derive(Debug, Clone)]
pub struct OracleCc {
    /// Fixed congestion window in bytes. On the emulator (no bandwidth limit) a
    /// large value lets QUIC run at link rate; on a real mixnet this should be
    /// the bandwidth-delay product derived from the oracle.
    pub window: u64,
}

impl ControllerFactory for OracleCc {
    fn build(self: Arc<Self>, _now: Instant, _current_mtu: u16) -> Box<dyn Controller> {
        Box::new(OracleController {
            window: self.window,
        })
    }
}
