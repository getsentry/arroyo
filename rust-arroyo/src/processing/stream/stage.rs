use std::future::Future;
use std::sync::Arc;

use crate::backends::kafka::types::KafkaPayload;

use super::pipeline_envelope::{MessageMetadata, PipelineEnvelope};

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
        raw: Arc<KafkaPayload>,
        reason: RejectionReason,
    },

    /// Unrecoverable error — kill the pipeline.
    Fail(Box<dyn std::error::Error + Send>),

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

    fn name(&self) -> &'static str;
}
