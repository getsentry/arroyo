use crate::processing::strategies::{
    merge_commit_request, CommitRequest, MessageRejected, ProcessingStrategy, StrategyError,
    SubmitError,
};
use crate::types::Message;
use std::time::Duration;

pub struct RunTask<TTransformed, F, N> {
    pub function: F,
    pub next_step: N,
    pub message_carried_over: Option<Message<TTransformed>>,
    pub commit_request_carried_over: Option<CommitRequest>,
}

impl<TTransformed, F, N> RunTask<TTransformed, F, N> {
    pub fn new(function: F, next_step: N) -> Self {
        Self {
            function,
            next_step,
            message_carried_over: None,
            commit_request_carried_over: None,
        }
    }
}

impl<TPayload, TTransformed, F, N> ProcessingStrategy<TPayload> for RunTask<TTransformed, F, N>
where
    TTransformed: Send + Sync,
    F: FnMut(Message<TPayload>) -> Result<Message<TTransformed>, SubmitError<TPayload>>
        + Send
        + Sync
        + 'static,
    N: ProcessingStrategy<TTransformed> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        match self.next_step.poll() {
            Ok(commit_request) => {
                self.commit_request_carried_over =
                    merge_commit_request(self.commit_request_carried_over.take(), commit_request)
            }
            Err(invalid_message) => return Err(invalid_message),
        }

        if let Some(message) = self.message_carried_over.take() {
            match self.next_step.submit(message) {
                Err(SubmitError::MessageRejected(MessageRejected {
                    message: transformed_message,
                })) => {
                    self.message_carried_over = Some(transformed_message);
                }
                Err(SubmitError::InvalidMessage(invalid_message)) => {
                    return Err(invalid_message.into());
                }
                Ok(_) => {}
            }
        }

        Ok(self.commit_request_carried_over.take())
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), SubmitError<TPayload>> {
        if self.message_carried_over.is_some() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        let next_message = (self.function)(message)?;

        match self.next_step.submit(next_message) {
            Err(SubmitError::MessageRejected(MessageRejected {
                message: transformed_message,
            })) => {
                self.message_carried_over = Some(transformed_message);
            }
            Err(SubmitError::InvalidMessage(invalid_message)) => {
                return Err(SubmitError::InvalidMessage(invalid_message));
            }
            Ok(_) => {}
        }
        Ok(())
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let next_commit = self.next_step.join(timeout)?;
        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            next_commit,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        processing::strategies::noop::Noop,
        types::{BrokerMessage, InnerMessage, Message, Partition, Topic},
    };
    use chrono::Utc;

    #[test]
    fn test_run_task() {
        fn identity(value: Message<String>) -> Result<Message<String>, SubmitError<String>> {
            Ok(value)
        }

        let mut strategy = RunTask::new(identity, Noop {});

        let partition = Partition::new(Topic::new("test"), 0);

        strategy
            .submit(Message {
                inner_message: InnerMessage::BrokerMessage(BrokerMessage::new(
                    "Hello world".to_string(),
                    partition,
                    0,
                    Utc::now(),
                )),
            })
            .unwrap();
    }
}
