use super::error_handler::{ErrorContext, ErrorHandler};

/// A canned ErrorHandler that logs the error and continues.
/// Use when no DLQ is configured.
pub struct LogErrorHandler;

impl ErrorHandler for LogErrorHandler {
    async fn handle(
        &self,
        error: ErrorContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        tracing::error!(
            "Unhandled pipeline error at {:?}:{} kind={} error={}",
            error.origin.partition,
            error.origin.offset,
            error.kind,
            error.source,
        );
        Ok(())
    }
}
