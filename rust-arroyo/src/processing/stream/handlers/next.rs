use std::future::Future;

use super::super::pipeline_envelope::PipelineEnvelope;

/// Handler for successfully processed messages.
/// Called by the pipeline's on_ok() combinator for each Emit envelope.
pub trait NextHandler<T>: Send + Sync {
    fn handle(
        &self,
        envelope: &PipelineEnvelope<T>,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send>>> + Send;
}
