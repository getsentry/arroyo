use std::time::Duration;

use crate::processing::stream::offset_tracker::{OffsetCommitter, OffsetTracker};
use crate::processing::stream::pipeline_envelope::PipelineEnvelope;
use crate::processing::stream::pipeline_envelope::MessageMetadata;

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

impl<T> StreamCollector<T> for OffsetCollector<'_> {
    fn on_emit(&mut self, envelope: &PipelineEnvelope<T>) {
        self.tracker
            .track(envelope.metadata.partition, envelope.metadata.offset + 1);
        self.tracker.record_latency(envelope.metadata.timestamp);
        let _ = self.tracker.maybe_commit();
    }

    fn on_drop(&mut self, metadata: &MessageMetadata) {
        self.tracker.track(metadata.partition, metadata.offset + 1);
        let _ = self.tracker.maybe_commit();
    }

    fn on_reject(&mut self, metadata: &MessageMetadata) {
        self.tracker.track(metadata.partition, metadata.offset + 1);
        let _ = self.tracker.maybe_commit();
    }

    fn on_complete(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.tracker.flush()
    }
}
