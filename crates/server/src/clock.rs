//! The server's wall-clock seam.
//!
//! Time-driven behavior — the awareness reconnect-grace window, and later TTL
//! expiry and throttle — reads the clock through this trait so it can be driven
//! deterministically in tests. Production uses [`SystemClock`]; tests drive a
//! [`ManualClock`] and advance it by hand.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A source of monotonic-enough wall time in milliseconds since the epoch.
pub trait Clock: Send + Sync {
    fn now_millis(&self) -> u64;
}

/// Wall time from the operating system.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// A clock the caller sets by hand, for driving time-dependent behavior in
/// tests without sleeping.
#[derive(Default)]
pub struct ManualClock {
    now: AtomicU64,
}

impl ManualClock {
    /// A clock reading `start` milliseconds.
    pub fn new(start: u64) -> Self {
        Self {
            now: AtomicU64::new(start),
        }
    }

    /// Move the clock forward by `millis`.
    pub fn advance(&self, millis: u64) {
        self.now.fetch_add(millis, Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.now.load(Ordering::Relaxed)
    }
}
