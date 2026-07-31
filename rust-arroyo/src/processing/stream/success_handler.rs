use std::future::Future;

use super::envelope::Envelope;

/// Handler for successfully processed messages.
/// Called by the pipeline's on_ok() combinator for each Ok envelope.
///
/// Implementations should perform side effects (e.g. produce to Kafka)
/// and return Ok(()) on success. If the handler returns Err, the pipeline
/// routes the failure to the ErrorHandler.
pub trait SuccessHandler<T>: Send + Sync {
    fn handle(
        &self,
        envelope: &Envelope<T>,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send>>> + Send;
}
