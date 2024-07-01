use std::time::Duration;

use crate::types::Message;

use super::{CommitRequest, ProcessingStrategy, StrategyError, SubmitError};

/// Noop strategy that takes a message and does nothing.
///
/// This can be useful when you do not care to commit an offset.
pub struct Noop {}

impl<TPayload> ProcessingStrategy<TPayload> for Noop {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(None)
    }
    fn submit(&mut self, _message: Message<TPayload>) -> Result<(), SubmitError<TPayload>> {
        Ok(())
    }
    fn terminate(&mut self) {}
    fn join(&mut self, _timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(None)
    }
}
