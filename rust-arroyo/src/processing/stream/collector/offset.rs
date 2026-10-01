use std::time::Duration;

use crate::processing::stream::offset_tracker::{OffsetCommitter, OffsetTracker};
use crate::processing::stream::pipeline_envelope::MessageMetadata;
use crate::processing::stream::pipeline_envelope::PipelineEnvelope;
use crate::processing::stream::stage::PipelineExit;
use crate::processing::stream::BoxError;

use super::stream_collector::StreamCollector;

/// Collector that tracks offsets and commits them periodically.
/// The production collector for Kafka consumers.
pub struct OffsetCollector<'a> {
    tracker: OffsetTracker<'a>,
}

impl<'a> OffsetCollector<'a> {
    pub fn new(committer: &'a dyn OffsetCommitter, commit_interval: Duration) -> Self {
        Self {
            tracker: OffsetTracker::new(commit_interval, committer),
        }
    }
}

/// Note: Dropped and Rejected messages do not advance offsets.
///
/// Batching is the blocker: a Drop at offset 5 would commit before a
/// batch spanning offsets 0-4 flushes, rewinding the committed offset.
/// The next Emit (batch flush) implicitly covers dropped offsets with a
/// higher commit. This matches the push model, where only messages that
/// reach `CommitOffsets` advance the offset.
impl<T> StreamCollector<T> for OffsetCollector<'_> {
    fn on_emit(&mut self, envelope: &PipelineEnvelope<T>) {
        for (partition, offset) in &envelope.offsets {
            self.tracker.track(*partition, offset + 1);
        }
        self.tracker.record_latency(envelope.metadata.timestamp);
    }

    fn on_drop(&mut self, _metadata: &MessageMetadata) {}

    fn on_reject(&mut self, _metadata: &MessageMetadata) {}

    fn on_fail(&mut self, _error: &BoxError) -> Result<(), BoxError> {
        self.tracker.flush()
    }

    fn on_exit(&mut self, _reason: PipelineExit) -> Result<(), BoxError> {
        self.tracker.flush()
    }

    fn on_complete(&mut self) -> Result<(), BoxError> {
        self.tracker.flush()
    }

    fn after_each(&mut self) -> Result<(), BoxError> {
        self.tracker.maybe_commit()
    }
}
