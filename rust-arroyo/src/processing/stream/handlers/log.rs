use crate::processing::stream::BoxError;

use super::rejection::{RejectionHandler, RejectionMetadata};

/// A canned RejectionHandler that logs the rejection and continues.
pub struct LogHandler;

impl RejectionHandler for LogHandler {
    async fn handle(&self, rejected: &RejectionMetadata) -> Result<(), BoxError> {
        tracing::error!(
            "Rejected message at {:?}:{} reason={:?}",
            rejected.metadata.partition,
            rejected.metadata.offset,
            rejected.reason,
        );
        Ok(())
    }
}
