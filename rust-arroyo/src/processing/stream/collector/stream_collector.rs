use crate::processing::stream::pipeline_envelope::{MessageMetadata, PipelineEnvelope};
use crate::processing::stream::stage::PipelineExit;
use crate::processing::stream::BoxError;

/// Receives pipeline events from the `run()` terminal combinator.
///
/// Implement this to control what happens when items are emitted,
/// dropped, or rejected. `OffsetCollector` tracks offsets for Kafka
/// commit. `NoopCollector` drains without side-effects.
///
/// One method per `StageResult` variant, plus:
///   - `after_each` — runs once per item, whatever the variant
///   - `on_complete` — the stream ended without a terminal variant
///     (an exhausted finite source)
pub trait StreamCollector<T>: Send + Sync {
    fn on_emit(&mut self, envelope: &PipelineEnvelope<T>);
    fn on_drop(&mut self, metadata: &MessageMetadata);
    fn on_reject(&mut self, metadata: &MessageMetadata);
    fn on_fail(&mut self, error: &BoxError) -> Result<(), BoxError>;
    fn on_exit(&mut self, reason: PipelineExit) -> Result<(), BoxError>;
    fn on_complete(&mut self) -> Result<(), BoxError>;
    fn after_each(&mut self) -> Result<(), BoxError>;
}
