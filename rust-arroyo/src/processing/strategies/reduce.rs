use crate::processing::strategies::{
    merge_commit_request, CommitRequest, MessageRejected, ProcessingStrategy, StrategyError,
    SubmitError,
};
use crate::timer;
use crate::types::{Message, Partition};
use crate::utils::timing::Deadline;
use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use super::InvalidMessage;

struct BatchState<T, TResult> {
    value: Option<TResult>,
    accumulator: Arc<
        dyn Fn(TResult, Message<T>) -> Result<TResult, (SubmitError<T>, TResult)> + Send + Sync,
    >,
    offsets: BTreeMap<Partition, u64>,
    batch_start_time: Deadline,
    message_count: usize,
    compute_batch_size: fn(&T) -> usize,
}

impl<T, TResult> BatchState<T, TResult> {
    fn new(
        initial_value: TResult,
        accumulator: Arc<
            dyn Fn(TResult, Message<T>) -> Result<TResult, (SubmitError<T>, TResult)> + Send + Sync,
        >,
        max_batch_time: Duration,
        compute_batch_size: fn(&T) -> usize,
    ) -> BatchState<T, TResult> {
        BatchState {
            value: Some(initial_value),
            accumulator,
            offsets: Default::default(),
            batch_start_time: Deadline::new(max_batch_time),
            message_count: 0,
            compute_batch_size,
        }
    }

    fn add(&mut self, message: Message<T>) -> Result<(), SubmitError<T>> {
        let commitable: Vec<_> = message.committable().collect();
        let message_count = (self.compute_batch_size)(&message.payload());
        let prev_result = self.value.take().unwrap();

        match (self.accumulator)(prev_result, message) {
            Ok(result) => {
                self.value = Some(result);
                self.message_count += message_count;
                for (partition, offset) in commitable {
                    self.offsets.insert(partition, offset);
                }
                Ok(())
            }
            Err((submit_error, prev_result)) => {
                self.value = Some(prev_result);
                Err(submit_error)
            }
        }
    }
}

pub struct Reduce<T, TResult> {
    next_step: Box<dyn ProcessingStrategy<TResult>>,
    accumulator: Arc<
        dyn Fn(TResult, Message<T>) -> Result<TResult, (SubmitError<T>, TResult)> + Send + Sync,
    >,
    initial_value: Arc<dyn Fn() -> TResult + Send + Sync>,
    max_batch_size: usize,
    max_batch_time: Duration,
    batch_state: BatchState<T, TResult>,
    message_carried_over: Option<Message<TResult>>,
    commit_request_carried_over: Option<CommitRequest>,
    compute_batch_size: fn(&T) -> usize,
    flush_empty_batches: bool,
}

impl<T: Send + Sync, TResult: Send + Sync> ProcessingStrategy<T> for Reduce<T, TResult> {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let commit_request = self.next_step.poll()?;
        self.commit_request_carried_over =
            merge_commit_request(self.commit_request_carried_over.take(), commit_request);

        self.flush(false)?;

        Ok(self.commit_request_carried_over.take())
    }

    fn submit(&mut self, message: Message<T>) -> Result<(), SubmitError<T>> {
        if self.message_carried_over.is_some() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        self.batch_state.add(message)?;

        Ok(())
    }

    fn terminate(&mut self) {
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let deadline = timeout.map(Deadline::new);

        loop {
            if deadline.map_or(false, |d| d.has_elapsed()) {
                tracing::warn!(
                    "Timeout {:?} reached while waiting for tasks to finish",
                    timeout
                );
                break;
            }

            let next_commit = self.next_step.poll()?;
            self.commit_request_carried_over =
                merge_commit_request(self.commit_request_carried_over.take(), next_commit);

            self.flush(true)?;

            if self.message_carried_over.is_none() {
                break;
            }
        }

        let next_commit = self.next_step.join(deadline.map(|d| d.remaining()))?;

        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            next_commit,
        ))
    }
}

impl<T, TResult> Reduce<T, TResult> {
    pub fn new<N>(
        next_step: N,
        accumulator: Arc<
            dyn Fn(TResult, Message<T>) -> Result<TResult, (SubmitError<T>, TResult)> + Send + Sync,
        >,
        initial_value: Arc<dyn Fn() -> TResult + Send + Sync>,
        max_batch_size: usize,
        max_batch_time: Duration,
        compute_batch_size: fn(&T) -> usize,
    ) -> Self
    where
        N: ProcessingStrategy<TResult> + 'static,
    {
        let batch_state = BatchState::new(
            (initial_value)(),
            accumulator.clone(),
            max_batch_time,
            compute_batch_size,
        );
        Reduce {
            next_step: Box::new(next_step),
            accumulator,
            initial_value,
            max_batch_size,
            max_batch_time,
            batch_state,
            message_carried_over: None,
            commit_request_carried_over: None,
            compute_batch_size,
            flush_empty_batches: false,
        }
    }

    pub fn flush_empty_batches(mut self, yes: bool) -> Self {
        self.flush_empty_batches = yes;
        self
    }

    fn flush(&mut self, force: bool) -> Result<(), InvalidMessage> {
        // Try re-submitting the carried over message if there is one
        if let Some(message) = self.message_carried_over.take() {
            match self.next_step.submit(message) {
                Err(SubmitError::MessageRejected(MessageRejected {
                    message: transformed_message,
                })) => {
                    self.message_carried_over = Some(transformed_message);
                }
                Err(SubmitError::InvalidMessage(invalid_message)) => {
                    return Err(invalid_message);
                }
                Ok(_) => {}
            }
        }

        if self.batch_state.message_count == 0 && !self.flush_empty_batches {
            return Ok(());
        }

        let batch_time = self.batch_state.batch_start_time.elapsed();
        let batch_complete = self.batch_state.message_count >= self.max_batch_size
            || batch_time >= self.max_batch_time;

        if !batch_complete && !force {
            return Ok(());
        }

        timer!("arroyo.strategies.reduce.batch_time.ms", batch_time);

        let batch_state = mem::replace(
            &mut self.batch_state,
            BatchState::new(
                (self.initial_value)(),
                self.accumulator.clone(),
                self.max_batch_time,
                self.compute_batch_size,
            ),
        );

        let next_message =
            Message::new_any_message(batch_state.value.unwrap(), batch_state.offsets);

        match self.next_step.submit(next_message) {
            Err(SubmitError::MessageRejected(MessageRejected {
                message: transformed_message,
            })) => {
                self.message_carried_over = Some(transformed_message);
                Ok(())
            }
            Err(SubmitError::InvalidMessage(invalid_message)) => Err(invalid_message),
            Ok(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::processing::strategies::reduce::Reduce;
    use crate::processing::strategies::{
        CommitRequest, MessageRejected, ProcessingStrategy, StrategyError, SubmitError,
    };
    use crate::types::{BrokerMessage, InnerMessage, Message, Partition, Topic};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    struct NextStep<T> {
        pub submitted: Arc<Mutex<Vec<T>>>,
    }

    impl<T: Send + Sync> ProcessingStrategy<T> for NextStep<T> {
        fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }

        fn submit(&mut self, message: Message<T>) -> Result<(), SubmitError<T>> {
            self.submitted.lock().unwrap().push(message.into_payload());
            Ok(())
        }

        fn terminate(&mut self) {}

        fn join(&mut self, _: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
            Ok(None)
        }
    }

    #[test]
    fn test_reduce() {
        let submitted_messages = Arc::new(Mutex::new(Vec::new()));
        let submitted_messages_clone = submitted_messages.clone();

        let partition1 = Partition::new(Topic::new("test"), 0);

        let max_batch_size = 2;
        let max_batch_time = Duration::from_secs(1);

        let initial_value = Vec::new();
        let accumulator = Arc::new(|mut acc: Vec<u64>, msg: Message<u64>| {
            acc.push(msg.into_payload());
            Ok(acc)
        });
        let compute_batch_size = |_: &_| -> usize { 1 };

        let next_step = NextStep {
            submitted: submitted_messages,
        };

        let mut strategy = Reduce::new(
            next_step,
            accumulator,
            Arc::new(move || initial_value.clone()),
            max_batch_size,
            max_batch_time,
            compute_batch_size,
        );

        for i in 0..3 {
            let msg = Message {
                inner_message: InnerMessage::BrokerMessage(BrokerMessage::new(
                    i,
                    partition1,
                    i,
                    chrono::Utc::now(),
                )),
            };
            strategy.submit(msg).unwrap();
            let _ = strategy.poll();
        }

        // 3 messages with a max batch size of 2 means 1 batch was cleared
        // and 1 message is left before next size limit.
        assert_eq!(strategy.batch_state.message_count, 1);

        let _ = strategy.join(None);

        // 2 batches were created
        assert_eq!(
            *submitted_messages_clone.lock().unwrap(),
            vec![vec![0, 1], vec![2]]
        );
    }

    #[test]
    fn test_reduce_with_backpressure() {
        let submitted_messages = Arc::new(Mutex::new(Vec::new()));
        let submitted_messages_clone = submitted_messages.clone();

        let partition1 = Partition::new(Topic::new("test"), 0);

        let max_batch_size = 2;
        let max_batch_time = Duration::from_secs(1);

        #[derive(Clone, Debug, PartialEq)]
        struct Buffer<T> {
            data: Vec<T>,
            flushed: bool,
        }

        let initial_value = Buffer {
            data: Vec::new(),
            flushed: false,
        };

        let accumulator = Arc::new(move |mut acc: Buffer<u64>, msg: Message<u64>| {
            if acc.flushed {
                acc.data.push(msg.into_payload());
                acc.flushed = false;
                Ok(acc)
            } else {
                acc.flushed = true;
                Err((
                    SubmitError::MessageRejected(MessageRejected { message: msg }),
                    acc,
                ))
            }
        });
        let compute_batch_size = |_: &_| -> usize { 1 };

        let next_step = NextStep {
            submitted: submitted_messages,
        };

        let mut strategy = Reduce::new(
            next_step,
            accumulator,
            Arc::new(move || initial_value.clone()),
            max_batch_size,
            max_batch_time,
            compute_batch_size,
        );

        for i in 0..3 {
            let msg = Message {
                inner_message: InnerMessage::BrokerMessage(BrokerMessage::new(
                    i,
                    partition1,
                    i,
                    chrono::Utc::now(),
                )),
            };
            let res = strategy.submit(msg);
            match res {
                Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                    strategy.submit(message).unwrap();
                }
                _ => {
                    unreachable!("Strategy should have backpressured")
                }
            };
            let _ = strategy.poll();
        }

        // 3 messages with a max batch size of 2 means 1 batch was cleared
        // and 1 message is left before next size limit.
        assert_eq!(strategy.batch_state.message_count, 1);

        strategy.close();
        let _ = strategy.join(None);

        // 2 batches were created
        assert_eq!(
            *submitted_messages_clone.lock().unwrap(),
            vec![
                Buffer {
                    data: vec![0, 1],
                    flushed: false
                },
                Buffer {
                    data: vec![2],
                    flushed: false
                }
            ]
        );
    }

    #[test]
    fn test_reduce_with_custom_batch_size() {
        let submitted_messages = Arc::new(Mutex::new(Vec::new()));
        let submitted_messages_clone = submitted_messages.clone();

        let partition1 = Partition::new(Topic::new("test"), 0);

        let max_batch_size = 10;
        let max_batch_time = Duration::from_secs(1);

        let initial_value = Vec::new();
        let accumulator = Arc::new(|mut acc: Vec<u64>, msg: Message<u64>| {
            acc.push(msg.into_payload());
            Ok(acc)
        });
        let compute_batch_size = |_: &_| -> usize { 5 };

        let next_step = NextStep {
            submitted: submitted_messages,
        };

        let mut strategy = Reduce::new(
            next_step,
            accumulator,
            Arc::new(move || initial_value.clone()),
            max_batch_size,
            max_batch_time,
            compute_batch_size,
        );

        for i in 0..3 {
            let msg = Message {
                inner_message: InnerMessage::BrokerMessage(BrokerMessage::new(
                    i,
                    partition1,
                    i,
                    chrono::Utc::now(),
                )),
            };
            strategy.submit(msg).unwrap();
            let _ = strategy.poll();
        }

        // 3 messages returning 5 items each and a max batch size of 10
        // means 1 batch was cleared and 5 items are in the current batch.
        assert_eq!(strategy.batch_state.message_count, 5);

        let _ = strategy.join(None);

        // 2 batches were created
        assert_eq!(
            *submitted_messages_clone.lock().unwrap(),
            vec![vec![0, 1], vec![2]]
        );
    }

    #[test]
    fn test_reduce_with_zero_batch_size() {
        let submitted_messages = Arc::new(Mutex::new(Vec::new()));
        let submitted_messages_clone = submitted_messages.clone();

        let partition1 = Partition::new(Topic::new("test"), 0);

        let max_batch_size = 1;
        let max_batch_time = Duration::from_secs(100);

        let initial_value = Vec::new();
        let accumulator = Arc::new(|mut acc: Vec<u64>, msg: Message<u64>| {
            acc.push(msg.into_payload());
            Ok(acc)
        });
        let compute_batch_size = |_: &_| -> usize { 0 };

        let next_step = NextStep {
            submitted: submitted_messages,
        };

        let mut strategy = Reduce::new(
            next_step,
            accumulator,
            Arc::new(move || initial_value.clone()),
            max_batch_size,
            max_batch_time,
            compute_batch_size,
        );

        for i in 0..3 {
            let msg = Message {
                inner_message: InnerMessage::BrokerMessage(BrokerMessage::new(
                    i,
                    partition1,
                    i,
                    chrono::Utc::now(),
                )),
            };
            strategy.submit(msg).unwrap();
            let _ = strategy.poll();
        }

        // since all submitted values had length 0, do not forward any messages to the next step
        // until timeout (which will not happen as part of this test)
        assert_eq!(strategy.batch_state.message_count, 0);

        let _ = strategy.join(None);

        // no batches were created
        assert!(submitted_messages_clone.lock().unwrap().is_empty());
    }

    #[test]
    fn test_reduce_with_zero_batch_size_flush() {
        let submitted_messages = Arc::new(Mutex::new(Vec::new()));
        let submitted_messages_clone = submitted_messages.clone();

        let partition1 = Partition::new(Topic::new("test"), 0);

        let max_batch_size = 1;
        let max_batch_time = Duration::from_secs(100);

        let initial_value = Vec::new();
        let accumulator = Arc::new(|mut acc: Vec<u64>, msg: Message<u64>| {
            acc.push(msg.into_payload());
            Ok(acc)
        });
        let compute_batch_size = |_: &_| -> usize { 0 };

        let next_step = NextStep {
            submitted: submitted_messages,
        };

        let mut strategy = Reduce::new(
            next_step,
            accumulator,
            Arc::new(move || initial_value.clone()),
            max_batch_size,
            max_batch_time,
            compute_batch_size,
        )
        .flush_empty_batches(true);

        for i in 0..3 {
            let msg = Message {
                inner_message: InnerMessage::BrokerMessage(BrokerMessage::new(
                    i,
                    partition1,
                    i,
                    chrono::Utc::now(),
                )),
            };
            strategy.submit(msg).unwrap();
            let _ = strategy.poll();
        }

        // since all submitted values had length 0, do not forward any messages to the next step
        // until timeout (which will not happen as part of this test)
        assert_eq!(strategy.batch_state.message_count, 0);

        let _ = strategy.join(None);

        // "empty" batch was created -- flushed even though the batch size callback claims it is of
        // size zero
        assert_eq!(
            *submitted_messages_clone.lock().unwrap(),
            vec![vec![0, 1, 2]]
        );
    }
}
