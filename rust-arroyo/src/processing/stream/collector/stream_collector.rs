use crate::processing::stream::pipeline_envelope::{MessageMetadata, PipelineEnvelope};
use crate::processing::stream::BoxError;

/// Receives pipeline events from the `run()` terminal combinator.
///
/// Implement this to control what happens when items are emitted,
/// dropped, or rejected. `OffsetCollector` tracks offsets for Kafka
/// commit. `NoopCollector` drains without side-effects.
pub trait StreamCollector<T>: Send + Sync {
    fn on_emit(&mut self, envelope: &PipelineEnvelope<T>);
    fn on_drop(&mut self, metadata: &MessageMetadata);
    fn on_reject(&mut self, metadata: &MessageMetadata);
    fn on_complete(&mut self) -> Result<(), BoxError>;
}
