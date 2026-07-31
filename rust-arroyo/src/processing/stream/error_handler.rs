use std::fmt;
use std::future::Future;
use std::sync::Arc;

use crate::backends::kafka::types::KafkaPayload;
use crate::types::BrokerMessage;

/// Context provided to the ErrorHandler when something goes wrong.
/// Created by the pipeline — the user never constructs this.
pub struct ErrorContext {
    /// The original Kafka message — always available for DLQ.
    pub origin: Arc<BrokerMessage<KafkaPayload>>,

    /// What went wrong.
    pub source: Box<dyn std::error::Error + Send>,

    /// What kind of error this is.
    pub kind: ErrorKind,
}

/// Classification of the error — lets the handler decide what to do.
#[derive(Debug, Clone, Copy)]
pub enum ErrorKind {
    /// A stage said the message is bad (unparseable, invalid schema, etc.)
    InvalidMessage,

    /// The success handler (on_ok) failed — e.g. Kafka produce failed.
    ProduceFailure,

    /// Unrecoverable error from a stage.
    Fatal,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::InvalidMessage => write!(f, "invalid_message"),
            ErrorKind::ProduceFailure => write!(f, "produce_failure"),
            ErrorKind::Fatal => write!(f, "fatal"),
        }
    }
}

/// Handler for pipeline errors. Called by the pipeline's on_error() combinator.
///
/// Receives an ErrorContext with the original message, the error, and the error kind.
/// Returns Ok(()) if the error was handled (e.g. produced to DLQ).
/// Returns Err if the error handler itself failed — the pipeline will stop.
pub trait ErrorHandler: Send + Sync {
    fn handle(
        &self,
        error: ErrorContext,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send>>> + Send;
}
