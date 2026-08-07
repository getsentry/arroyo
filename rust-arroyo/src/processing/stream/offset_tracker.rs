use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::timer;
use crate::types::Partition;

/// Trait for committing offsets. KafkaSource implements this.
/// Tests can provide a mock.
pub trait OffsetCommitter: Send + Sync {
    fn commit_offsets(
        &self,
        positions: &HashMap<Partition, u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;
}

/// Tracks offsets per partition and commits them on a time-throttled interval.
pub struct OffsetTracker<'a> {
    committer: &'a dyn OffsetCommitter,
    offsets: HashMap<Partition, u64>,
    last_commit_time: coarsetime::Instant,
    last_record_time: coarsetime::Instant,
    commit_frequency: coarsetime::Duration,
    // Held to keep the background time updater alive. Without this,
    // coarsetime::Instant::recent() would never advance.
    _updater: coarsetime::Updater,
}

impl<'a> OffsetTracker<'a> {
    pub fn new(commit_frequency: Duration, committer: &'a dyn OffsetCommitter) -> Self {
        Self {
            committer,
            offsets: Default::default(),
            last_commit_time: coarsetime::Instant::recent(),
            last_record_time: coarsetime::Instant::recent(),
            commit_frequency: commit_frequency.into(),
            _updater: coarsetime::Updater::new(10).start().unwrap(),
        }
    }

    /// Record the offset for a partition.
    pub fn track(&mut self, partition: Partition, offset: u64) {
        self.offsets.insert(partition, offset);
    }

    /// Record a message timestamp for latency tracking.
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

    /// Commit offsets if the commit frequency has elapsed.
    pub fn maybe_commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.try_commit(false)
    }

    /// Commit all tracked offsets regardless of timing.
    pub fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.try_commit(true)
    }

    fn try_commit(&mut self, force: bool) -> Result<(), Box<dyn std::error::Error + Send>> {
        if self.offsets.is_empty() {
            return Ok(());
        }

        if !force
            && coarsetime::Instant::recent() - self.last_commit_time <= self.commit_frequency
        {
            return Ok(());
        }

        self.committer.commit_offsets(&self.offsets)?;
        self.offsets.clear();
        self.last_commit_time = coarsetime::Instant::recent();
        Ok(())
    }
}
