use crate::processing::stream::pipeline_envelope::{MessageMetadata, PipelineEnvelope};

use super::stream_collector::StreamCollector;

/// Collector that does nothing. For tests that just need to drain
/// the pipeline without tracking offsets.
pub struct NoopCollector;

impl<T> StreamCollector<T> for NoopCollector {
    fn on_emit(&mut self, _: &PipelineEnvelope<T>) {}
    fn on_drop(&mut self, _: &MessageMetadata) {}
    fn on_reject(&mut self, _: &MessageMetadata) {}
    fn on_complete(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
}
