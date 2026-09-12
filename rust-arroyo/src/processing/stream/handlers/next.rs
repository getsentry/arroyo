use std::future::Future;

use super::super::pipeline_envelope::PipelineEnvelope;
use crate::processing::stream::BoxError;

/// Handler for successfully processed messages.
/// Called by the pipeline's on_next() combinator for each Emit envelope.
pub trait NextHandler<T>: Send + Sync {
    fn handle(
        &self,
        envelope: &PipelineEnvelope<T>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send;
}
