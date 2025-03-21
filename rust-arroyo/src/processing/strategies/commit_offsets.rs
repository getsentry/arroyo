use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;

use crate::processing::strategies::{CommitRequest, ProcessingStrategy, SubmitError};
use crate::timer;
use crate::types::{Message, Partition};

use super::StrategyError;

pub struct CommitOffsets {
    partitions: HashMap<Partition, u64>,
    last_commit_time: coarsetime::Instant,
    last_record_time: coarsetime::Instant,
    commit_frequency: coarsetime::Duration,
}

impl CommitOffsets {
    pub fn new(commit_frequency: Duration) -> Self {
        CommitOffsets {
            partitions: Default::default(),
            last_commit_time: coarsetime::Instant::recent(),
            last_record_time: coarsetime::Instant::recent(),
            commit_frequency: commit_frequency.into(),
        }
    }

    fn commit(&mut self, force: bool) -> Option<CommitRequest> {
        // check if there is anything to commit first, since this is much cheaper than getting the
        // current time
        if self.partitions.is_empty() {
            return None;
        }

        if coarsetime::Instant::recent() - self.last_commit_time <= self.commit_frequency && !force
        {
            return None;
        }

        let ret = Some(CommitRequest {
            positions: self.partitions.clone(),
        });
        self.partitions.clear();
        self.last_commit_time = coarsetime::Instant::recent();
        ret
    }
}

impl<T> ProcessingStrategy<T> for CommitOffsets {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(self.commit(false))
    }

    fn submit(&mut self, message: Message<T>) -> Result<(), SubmitError<T>> {
        let now = coarsetime::Instant::recent();
        if now - self.last_record_time > coarsetime::Duration::from_secs(1) {
            if let Some(timestamp) = message.timestamp() {
                // FIXME: this used to be in seconds
                timer!(
                    "arroyo.consumer.latency",
                    (Utc::now() - timestamp).to_std().unwrap_or_default()
                );
                self.last_record_time = now;
            }
        }

        for (partition, offset) in message.committable() {
            self.partitions.insert(partition, offset);
        }
        Ok(())
    }

    fn terminate(&mut self) {}

    fn join(
        &mut self,
        _: Option<std::time::Duration>,
    ) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(self.commit(true))
    }
}

#[cfg(test)]
mod tests {
    use crate::backends::kafka::types::KafkaPayload;
    use crate::processing::strategies::commit_offsets::CommitOffsets;
    use crate::processing::strategies::{CommitRequest, ProcessingStrategy};

    use crate::types::{BrokerMessage, InnerMessage, Message, Partition, Topic};
    use chrono::DateTime;
    use std::thread::sleep;
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_commit_offsets() {
        tracing_subscriber::fmt().with_test_writer().init();
        let _updater = coarsetime::Updater::new(10).start().unwrap();

        let partition1 = Partition::new(Topic::new("noop-commit"), 0);
        let partition2 = Partition::new(Topic::new("noop-commit"), 1);
        let timestamp = DateTime::from(SystemTime::now());

        let m1 = Message {
            inner_message: InnerMessage::BrokerMessage(BrokerMessage {
                partition: partition1,
                offset: 1000,
                payload: KafkaPayload::new(None, None, None),
                timestamp,
            }),
        };

        let m2 = Message {
            inner_message: InnerMessage::BrokerMessage(BrokerMessage {
                partition: partition2,
                offset: 2000,
                payload: KafkaPayload::new(None, None, None),
                timestamp,
            }),
        };

        let mut noop = CommitOffsets::new(Duration::from_secs(1));

        let mut commit_req1 = CommitRequest {
            positions: Default::default(),
        };
        commit_req1.positions.insert(partition1, 1001);
        noop.submit(m1).expect("Failed to submit");
        assert_eq!(
            <CommitOffsets as ProcessingStrategy<KafkaPayload>>::poll(&mut noop).unwrap(),
            None
        );

        sleep(Duration::from_secs(2));
        assert_eq!(
            <CommitOffsets as ProcessingStrategy<KafkaPayload>>::poll(&mut noop).unwrap(),
            Some(commit_req1)
        );

        let mut commit_req2 = CommitRequest {
            positions: Default::default(),
        };
        commit_req2.positions.insert(partition2, 2001);
        noop.submit(m2).expect("Failed to submit");
        assert_eq!(
            <CommitOffsets as ProcessingStrategy<KafkaPayload>>::poll(&mut noop).unwrap(),
            None
        );
        assert_eq!(
            <CommitOffsets as ProcessingStrategy<KafkaPayload>>::join(
                &mut noop,
                Some(Duration::from_secs(5))
            )
            .unwrap(),
            Some(commit_req2)
        );
    }
}
