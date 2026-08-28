use std::future::Future;

use crate::backends::kafka::types::KafkaPayload;

use super::pipeline_envelope::{MessageMetadata, PipelineEnvelope};
use super::BoxError;

/// Why the pipeline stream ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineExit {
    /// Partition revocation — caller should recreate stages and restart.
    Rebalance,
    /// Graceful shutdown requested (SIGTERM/SIGINT).
    Shutdown,
    /// Source stream naturally ended (finite data, test sources).
    Complete,
}

/// The result of a Stage processing one envelope.
pub enum StageResult<T> {
    /// Produced output — pass downstream.
    Emit(PipelineEnvelope<T>),

    /// Evaluated and intentionally dropped (filtered).
    /// Carries metadata so the offset is still tracked.
    Drop { metadata: MessageMetadata },

    /// Equivalent to no result emission — offset is not propagated.
    /// Supports accumulating (batching).
    Skip,

    /// Rejected message — agnostic about reason.
    /// Carries metadata + raw for offset tracking and DLQ routing.
    Reject {
        metadata: MessageMetadata,
        raw: KafkaPayload,
        reason: RejectionReason,
    },

    /// Unrecoverable error — kill the pipeline.
    Fail(BoxError),

    /// Pipeline termination signal from the source.
    /// Passes through all combinators untouched until reaching commit().
    Exit(PipelineExit),
}

impl<T> StageResult<T> {
    /// Create a Reject result from an envelope, extracting metadata and raw.
    pub fn reject(envelope: PipelineEnvelope<T>, reason: RejectionReason) -> Self {
        StageResult::Reject {
            metadata: envelope.metadata,
            raw: envelope.raw,
            reason,
        }
    }

    /// Create a Drop result from an envelope, extracting metadata.
    pub fn drop(envelope: PipelineEnvelope<T>) -> Self {
        StageResult::Drop {
            metadata: envelope.metadata,
        }
    }

    /// Apply an async function to the Emit payload, passing all other
    /// variants through with a re-typed parameter.
    pub async fn map_emit<U, F, Fut>(self, f: F) -> StageResult<U>
    where
        F: FnOnce(PipelineEnvelope<T>) -> Fut,
        Fut: std::future::Future<Output = StageResult<U>>,
    {
        match self {
            StageResult::Emit(e) => f(e).await,
            StageResult::Drop { metadata } => StageResult::Drop { metadata },
            StageResult::Skip => StageResult::Skip,
            StageResult::Reject { metadata, raw, reason } => {
                StageResult::Reject { metadata, raw, reason }
            }
            StageResult::Fail(err) => StageResult::Fail(err),
            StageResult::Exit(reason) => StageResult::Exit(reason),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RejectionReason {
    /// Message is malformed or unparseable.
    Invalid,
    /// Message is valid but intentionally dropped (e.g. too old, load shedding).
    Ignored,
}

/// A processing stage in a pull-based pipeline.
///
/// Unified trait for all stage types:
///   - 1:1 transforms — return Emit(envelope)
///   - Filters — return Drop(metadata) to filter with offset tracking
///   - Batching — return Skip while accumulating, Emit when flushing
///   - Errors — return Reject or Fail
///
/// The framework handles error routing (DLQ), metrics, and offset tracking.
/// The process method is async to support stages that do I/O.
pub trait Stage: Send + Sync {
    type In: Send;
    type Out: Send;

    fn process(
        &self,
        envelope: PipelineEnvelope<Self::In>,
    ) -> impl Future<Output = StageResult<Self::Out>> + Send;

    fn name(&self) -> &str;
}

/// A stage that accumulates state and can be flushed externally.
///
/// Used by `apply_with_timer` to flush partial batches when a time
/// trigger fires. `BatchStage` implements this — most stages don't
/// need to.
pub trait FlushableStage: Stage {
    /// Flush accumulated state. Returns `Some(Emit(...))` if there
    /// is data to flush, `None` if the buffer is empty.
    fn flush(&self) -> Option<StageResult<Self::Out>>;
}
