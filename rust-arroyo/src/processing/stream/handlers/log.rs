use super::rejection::{RejectionMetadata, RejectionHandler};

/// A canned RejectionHandler that logs the rejection and continues.
pub struct LogHandler;

impl RejectionHandler for LogHandler {
    async fn handle(
        &self,
        rejected: &RejectionMetadata,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        tracing::error!(
            "Rejected message at {:?}:{} reason={:?}",
            rejected.metadata.partition,
            rejected.metadata.offset,
            rejected.reason,
        );
        Ok(())
    }
}
