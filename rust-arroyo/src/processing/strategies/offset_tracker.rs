use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::timer;
use crate::types::Partition;

/// Tracks offsets per partition and emits them on a time-throttled interval.
///
/// Shared by both the push-based CommitOffsets strategy and the pull-based
/// pipeline terminal. The offset tracking + time-throttled commit logic is
/// identical in both models.
pub struct OffsetTracker {
    offsets: HashMap<Partition, u64>,
    last_commit_time: coarsetime::Instant,
    last_record_time: coarsetime::Instant,
    commit_frequency: coarsetime::Duration,
}

impl OffsetTracker {
    pub fn new(commit_frequency: Duration) -> Self {
        Self {
            offsets: Default::default(),
            last_commit_time: coarsetime::Instant::recent(),
            last_record_time: coarsetime::Instant::recent(),
            commit_frequency: commit_frequency.into(),
        }
    }

    /// Record the offset for a partition. Keeps the highest offset per partition.
    pub fn track(&mut self, partition: Partition, offset: u64) {
        self.offsets.insert(partition, offset);
    }

    /// Record a message timestamp for latency tracking.
    /// Throttled to at most once per second.
    pub fn record_latency(&mut self, timestamp: DateTime<Utc>) {
        let now = coarsetime::Instant::recent();
        if now - self.last_record_time > coarsetime::Duration::from_secs(1) {
            timer!(
                "arroyo.consumer.latency",
                (Utc::now() - timestamp).to_std().unwrap_or_default()
            );
            self.last_record_time = now;
        }
    }

    /// Returns offsets to commit if the commit frequency has elapsed.
    /// Returns None if it's not time yet or there's nothing to commit.
    pub fn maybe_commit(&mut self) -> Option<HashMap<Partition, u64>> {
        self.try_commit(false)
    }

    /// Returns all tracked offsets regardless of timing. Use at shutdown
    /// or stream completion to ensure final offsets are committed.
    pub fn flush(&mut self) -> Option<HashMap<Partition, u64>> {
        self.try_commit(true)
    }

    fn try_commit(&mut self, force: bool) -> Option<HashMap<Partition, u64>> {
        if self.offsets.is_empty() {
            return None;
        }

        if !force
            && coarsetime::Instant::recent() - self.last_commit_time <= self.commit_frequency
        {
            return None;
        }

        let positions = self.offsets.clone();
        self.offsets.clear();
        self.last_commit_time = coarsetime::Instant::recent();
        Some(positions)
    }
}
