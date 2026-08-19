use std::time::Duration;

use tokio::time::Interval;

use crate::processing::stream::offset_tracker::ensure_time_updater;

/// A monotonic time source for `FlushTimer`.
///
/// Production uses `CoarseClock` (coarsetime's cached global, ~1ns reads).
/// Tests can inject a controllable clock for deterministic timing.
pub trait Clock: Send + Sync {
    fn now(&self) -> coarsetime::Instant;
}

/// Production clock — reads from coarsetime's cached global.
pub struct CoarseClock;

impl Clock for CoarseClock {
    fn now(&self) -> coarsetime::Instant {
        coarsetime::Instant::recent()
    }
}

/// Manages time-based flush triggers for `apply_with_timer`.
///
/// Uses coarsetime watermarks checked on each interval tick.
/// No per-item timer wheel operations.
///
/// Two triggers:
/// - **Idle**: flush if no upstream item arrives within `idle_timeout`.
///   Watermark `last_activity` resets on every item.
/// - **Cadence**: flush at `max_cadence` after the first item accumulated.
///   Watermark `batch_start` set once per batch.
pub struct FlushTimer<C: Clock> {
    clock: C,
    /// Interval tick — poll this in `select!`.
    pub interval: Interval,

    idle_dur: coarsetime::Duration,
    cadence_dur: coarsetime::Duration,

    batch_start: coarsetime::Instant,
    last_activity: coarsetime::Instant,
}

/// Far-future offset used as a sentinel — makes `is_active()` return false
/// and `should_flush()` return false without branches.
fn far_future() -> coarsetime::Duration {
    coarsetime::Duration::from_secs(365 * 24 * 3600)
}

impl FlushTimer<CoarseClock> {
    /// Create a production `FlushTimer` with coarsetime and a tokio interval.
    pub fn new(idle_timeout: Option<Duration>, max_cadence: Option<Duration>) -> Self {
        ensure_time_updater();

        let min_dur = [idle_timeout, max_cadence]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(Duration::from_secs(1));
        let tick = (min_dur / 10)
            .max(Duration::from_millis(1))
            .min(Duration::from_millis(100));

        Self::with_clock(
            CoarseClock,
            tokio::time::interval(tick),
            idle_timeout,
            max_cadence,
        )
    }
}

impl<C: Clock> FlushTimer<C> {
    /// Create a `FlushTimer` with an injected clock and interval.
    pub fn with_clock(
        clock: C,
        interval: Interval,
        idle_timeout: Option<Duration>,
        max_cadence: Option<Duration>,
    ) -> Self {
        let far = clock.now() + far_future();

        Self {
            clock,
            interval,
            idle_dur: idle_timeout.map_or(far_future(), |d| d.into()),
            cadence_dur: max_cadence.map_or(far_future(), |d| d.into()),
            batch_start: far,
            last_activity: far,
        }
    }

    /// Whether a batch is currently accumulating.
    pub fn is_active(&self) -> bool {
        self.batch_start <= self.clock.now()
    }

    /// An item was accumulated. Updates watermarks (~1ns).
    pub fn on_accumulate(&mut self) {
        let now = self.clock.now();
        self.last_activity = now;
        if self.batch_start > now {
            self.batch_start = now;
        }
    }

    /// The batch was flushed. Unsets watermarks.
    pub fn on_flush(&mut self) {
        self.unset();
    }

    /// Check if a flush trigger has fired based on watermarks.
    /// Called on each interval tick.
    pub fn should_flush(&self) -> bool {
        let now = self.clock.now();
        now.duration_since(self.batch_start) >= self.cadence_dur
            || now.duration_since(self.last_activity) >= self.idle_dur
    }

    /// Reset watermarks to far-future sentinels.
    fn unset(&mut self) {
        let far = self.clock.now() + far_future();
        self.batch_start = far;
        self.last_activity = far;
    }
}
