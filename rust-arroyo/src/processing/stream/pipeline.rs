use futures::stream::Stream;

use super::stage::StageResult;

use crate::backends::kafka::types::KafkaPayload;

/// A pull-based processing pipeline.
///
/// Implementors define the stage composition. The pipeline receives
/// a source stream and wires it through stages, consuming self so
/// the stages can be moved into the returned stream.
///
/// Stateful stages (like `BatchStage`) should be rebuilt each time
/// — create a fresh pipeline per partition assignment.
pub trait Pipeline: Send + Sync {
    type Output: Send;

    fn stream(
        self,
        source: impl Stream<Item = StageResult<KafkaPayload>> + Send,
    ) -> impl Stream<Item = StageResult<Self::Output>> + Send;
}
