use std::future::Future;
use std::sync::Arc;

use crate::backends::kafka::types::KafkaPayload;

use super::super::pipeline_envelope::MessageMetadata;
use super::super::stage::RejectionReason;

/// A rejected message, passed to the RejectionHandler.
pub struct RejectionMetadata {
    pub metadata: MessageMetadata,
    pub raw: Arc<KafkaPayload>,
    pub reason: RejectionReason,
}

/// Handler for rejected messages. Called by the pipeline's on_reject() combinator.
///
/// Receives a RejectionMetadata with metadata (for logging/headers),
/// raw payload (for DLQ produce), and the reason for rejection.
pub trait RejectionHandler: Send + Sync {
    fn handle(
        &self,
        rejected: &RejectionMetadata,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send>>> + Send;
}
