use std::future::Future;
use std::sync::Arc;

use crate::backends::kafka::types::KafkaPayload;
use crate::types::BrokerMessage;

use super::envelope::Envelope;

/// Error type for pipeline stages and handlers.
pub enum StageError {
    /// Bad message — the origin is preserved for the error handler.
    Invalid {
        origin: Arc<BrokerMessage<KafkaPayload>>,
        reason: InvalidReason,
    },

    /// Unrecoverable error — kills the pipeline.
    Fatal(Box<dyn std::error::Error + Send>),
}

#[derive(Debug, Clone, Copy)]
pub enum InvalidReason {
    /// Message is malformed or unparseable.
    Invalid,
    /// Message is valid but intentionally dropped (e.g. too old, load shedding).
    Ignored,
}

/// A processing stage in a pull-based pipeline.
///
/// Each stage takes an Envelope, processes it, and returns either a
/// transformed Envelope or a StageError. The framework handles error
/// routing (DLQ), metrics, and offset tracking.
///
/// The process method is async to support stages that do I/O.
pub trait Stage: Send + Sync {
    type In: Send;
    type Out: Send;

    fn process(
        &self,
        envelope: Envelope<Self::In>,
    ) -> impl Future<Output = Result<Envelope<Self::Out>, StageError>> + Send;

    fn name(&self) -> &'static str;
}
