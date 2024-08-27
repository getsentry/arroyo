use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::backends::kafka::producer::KafkaProducer;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer;
use crate::gauge;
use crate::types::{BrokerMessage, Partition, Topic, TopicOrPartition};

// This is a per-partition max
const MAX_PENDING_FUTURES: usize = 1000;

pub trait DlqProducer<TPayload>: Send + Sync {
    // Send a message to the DLQ.
    fn produce(
        &self,
        message: BrokerMessage<TPayload>,
    ) -> Pin<Box<dyn Future<Output = BrokerMessage<TPayload>> + Send + Sync>>;

    fn build_initial_state(
        &self,
        limit: DlqLimit,
        assignment: &HashMap<Partition, u64>,
    ) -> DlqLimitState;
}

// Drops all invalid messages. Produce returns an immediately resolved future.
struct NoopDlqProducer {}

impl<TPayload: Send + Sync + 'static> DlqProducer<TPayload> for NoopDlqProducer {
    fn produce(
        &self,
        message: BrokerMessage<TPayload>,
    ) -> Pin<Box<dyn Future<Output = BrokerMessage<TPayload>> + Send + Sync>> {
        Box::pin(async move { message })
    }

    fn build_initial_state(
        &self,
        limit: DlqLimit,
        _assignment: &HashMap<Partition, u64>,
    ) -> DlqLimitState {
        DlqLimitState::new(limit, HashMap::new())
    }
}

// KafkaDlqProducer forwards invalid messages to a Kafka topic

// Two additional fields are added to the headers of the Kafka message
// "original_partition": The partition of the original message
// "original_offset": The offset of the original message
pub struct KafkaDlqProducer {
    producer: Arc<KafkaProducer>,
    topic: TopicOrPartition,
}

impl KafkaDlqProducer {
    pub fn new(producer: KafkaProducer, topic: Topic) -> Self {
        Self {
            producer: Arc::new(producer),
            topic: TopicOrPartition::Topic(topic),
        }
    }
}

impl DlqProducer<KafkaPayload> for KafkaDlqProducer {
    fn produce(
        &self,
        message: BrokerMessage<KafkaPayload>,
    ) -> Pin<Box<dyn Future<Output = BrokerMessage<KafkaPayload>> + Send + Sync>> {
        let producer = self.producer.clone();
        let topic = self.topic;

        let mut headers = message.payload.headers().cloned().unwrap_or_default();

        headers = headers.insert(
            "original_partition",
            Some(message.offset.to_string().into_bytes()),
        );
        headers = headers.insert(
            "original_offset",
            Some(message.offset.to_string().into_bytes()),
        );

        let payload = KafkaPayload::new(
            message.payload.key().cloned(),
            Some(headers),
            message.payload.payload().cloned(),
        );

        Box::pin(async move {
            producer
                .produce(&topic, payload)
                .expect("Message was produced");

            message
        })
    }

    fn build_initial_state(
        &self,
        limit: DlqLimit,
        assignment: &HashMap<Partition, u64>,
    ) -> DlqLimitState {
        // XXX: We assume the last offsets were invalid when starting the consumer
        DlqLimitState::new(
            limit,
            assignment
                .iter()
                .filter_map(|(p, o)| {
                    Some((*p, o.checked_sub(1).map(InvalidMessageStats::invalid_at)?))
                })
                .collect(),
        )
    }
}

/// Defines any limits that should be placed on the number of messages that are
/// forwarded to the DLQ. This exists to prevent 100% of messages from going into
/// the DLQ if something is misconfigured or bad code is deployed. In this scenario,
/// it may be preferable to stop processing messages altogether and deploy a fix
/// rather than rerouting every message to the DLQ.
///
/// The ratio and max_consecutive_count are counted on a per-partition basis.
///
/// The default is no limit.
#[derive(Debug, Clone, Copy, Default)]
pub struct DlqLimit {
    pub max_invalid_ratio: Option<f64>,
    pub max_consecutive_count: Option<u64>,
}

/// A record of valid and invalid messages that have been received on a partition.
#[derive(Debug, Clone, Copy, Default)]
pub struct InvalidMessageStats {
    /// The number of valid messages that have been received.
    pub valid: u64,
    /// The number of invalid messages that have been received.
    pub invalid: u64,
    /// The length of the current run of received invalid messages.
    pub consecutive_invalid: u64,
    /// The offset of the last received invalid message.
    pub last_invalid_offset: u64,
}

impl InvalidMessageStats {
    /// Creates an empty record with the last invalid message received at `offset`.
    ///
    /// The `invalid` and `consecutive_invalid` fields are intentionally left at 0.
    pub fn invalid_at(offset: u64) -> Self {
        Self {
            last_invalid_offset: offset,
            ..Default::default()
        }
    }
}

/// Struct that keeps a record of how many valid and invalid messages have been received
/// per partition and decides whether to produce a message to the DLQ according to a configured limit.
#[derive(Debug, Clone, Default)]
pub struct DlqLimitState {
    limit: DlqLimit,
    records: HashMap<Partition, InvalidMessageStats>,
}

impl DlqLimitState {
    /// Creates a `DlqLimitState` with a given limit and initial set of records.
    pub fn new(limit: DlqLimit, records: HashMap<Partition, InvalidMessageStats>) -> Self {
        Self { limit, records }
    }

    /// Records an invalid message.
    ///
    /// This updates the internal statistics about the message's partition and
    /// returns `true` if the message should be produced to the DLQ according to the
    /// configured limit.
    fn record_invalid_message<T>(&mut self, message: &BrokerMessage<T>) -> bool {
        let record = self
            .records
            .entry(message.partition)
            .and_modify(|record| {
                let last_invalid = record.last_invalid_offset;
                match message.offset {
                    o if o <= last_invalid => {
                        tracing::error!("Invalid message raised out of order")
                    }
                    o if o == last_invalid + 1 => record.consecutive_invalid += 1,
                    o => {
                        let valid_count = o - last_invalid + 1;
                        record.valid += valid_count;
                        record.consecutive_invalid = 1;
                    }
                }

                record.invalid += 1;
                record.last_invalid_offset = message.offset;
            })
            .or_insert(InvalidMessageStats {
                valid: 0,
                invalid: 1,
                consecutive_invalid: 1,
                last_invalid_offset: message.offset,
            });

        if let Some(max_invalid_ratio) = self.limit.max_invalid_ratio {
            if record.valid == 0 {
                // When no valid messages have been processed, we should not
                // accept the message into the dlq. It could be an indicator
                // of severe problems on the pipeline. It is best to let the
                // consumer backlog in those cases.
                return false;
            }

            if (record.invalid as f64) / (record.valid as f64) > max_invalid_ratio {
                return false;
            }
        }

        if let Some(max_consecutive_count) = self.limit.max_consecutive_count {
            if record.consecutive_invalid > max_consecutive_count {
                return false;
            }
        }
        true
    }
}

/// DLQ policy defines the DLQ configuration, and is passed to the stream processor
/// upon creation of the consumer. It consists of the DLQ producer implementation and
/// any limits that should be applied.
pub struct DlqPolicy<TPayload> {
    handle: Handle,
    producer: Box<dyn DlqProducer<TPayload>>,
    limit: DlqLimit,
    max_buffered_messages_per_partition: Option<usize>,
}

impl<TPayload> DlqPolicy<TPayload> {
    pub fn new(
        handle: Handle,
        producer: Box<dyn DlqProducer<TPayload>>,
        limit: DlqLimit,
        max_buffered_messages_per_partition: Option<usize>,
    ) -> Self {
        DlqPolicy {
            handle,
            producer,
            limit,
            max_buffered_messages_per_partition,
        }
    }
}

impl<TPayload> fmt::Debug for DlqPolicy<TPayload> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DlqPolicy")
            .field("limit", &self.limit)
            .field(
                "max_buffered_messages_per_partition",
                &self.max_buffered_messages_per_partition,
            )
            .finish_non_exhaustive()
    }
}

// Wraps the DLQ policy and keeps track of messages pending produce/commit.
type Futures<TPayload> = VecDeque<(u64, JoinHandle<BrokerMessage<TPayload>>)>;

struct Inner<TPayload> {
    dlq_policy: DlqPolicy<TPayload>,
    dlq_limit_state: DlqLimitState,
    futures: BTreeMap<Partition, Futures<TPayload>>,
}

pub(crate) struct DlqPolicyWrapper<TPayload> {
    inner: Option<Inner<TPayload>>,
}

impl<TPayload: Send + Sync + 'static> DlqPolicyWrapper<TPayload> {
    pub fn new(dlq_policy: Option<DlqPolicy<TPayload>>) -> Self {
        let inner = dlq_policy.map(|dlq_policy| Inner {
            dlq_policy,
            dlq_limit_state: DlqLimitState::default(),
            futures: BTreeMap::new(),
        });
        Self { inner }
    }

    pub fn max_buffered_messages_per_partition(&self) -> Option<usize> {
        self.inner
            .as_ref()
            .and_then(|i| i.dlq_policy.max_buffered_messages_per_partition)
    }

    /// Clears the DLQ limits.
    pub fn reset_dlq_limits(&mut self, assignment: &HashMap<Partition, u64>) {
        let Some(inner) = self.inner.as_mut() else {
            return;
        };

        inner.dlq_limit_state = inner
            .dlq_policy
            .producer
            .build_initial_state(inner.dlq_policy.limit, assignment);
    }

    // Removes all completed futures, then appends a future with message to be produced
    // to the queue. Blocks if there are too many pending futures until some are done.
    pub fn produce(&mut self, message: BrokerMessage<TPayload>) {
        let Some(inner) = self.inner.as_mut() else {
            tracing::info!("dlq policy missing, dropping message");
            return;
        };
        for (_p, values) in inner.futures.iter_mut() {
            while !values.is_empty() {
                let len = values.len();
                let (_, future) = &mut values[0];
                if future.is_finished() || len >= MAX_PENDING_FUTURES {
                    let res = inner.dlq_policy.handle.block_on(future);
                    if let Err(err) = res {
                        tracing::error!("Error producing to DLQ: {}", err);
                    }
                    values.pop_front();
                } else {
                    break;
                }
            }
        }

        if inner.dlq_limit_state.record_invalid_message(&message) {
            tracing::info!("producing message to dlq");
            let (partition, offset) = (message.partition, message.offset);

            let task = inner.dlq_policy.producer.produce(message);
            let handle = inner.dlq_policy.handle.spawn(task);

            inner
                .futures
                .entry(partition)
                .or_default()
                .push_back((offset, handle));
        } else {
            panic!("DLQ limit was reached");
        }
    }

    // Blocks until all messages up to the committable have been produced so
    // they are safe to commit.
    pub fn flush(&mut self, committable: &HashMap<Partition, u64>) {
        let Some(inner) = self.inner.as_mut() else {
            return;
        };

        for (&p, &committable_offset) in committable {
            if let Some(values) = inner.futures.get_mut(&p) {
                while let Some((offset, future)) = values.front_mut() {
                    // The committable offset is message's offset + 1
                    if committable_offset > *offset {
                        if let Err(error) = inner.dlq_policy.handle.block_on(future) {
                            let error: &dyn std::error::Error = &error;
                            tracing::error!(error, "Error producing to DLQ");
                        }

                        values.pop_front();
                    } else {
                        break;
                    }
                }
            }
        }
    }
}

/// Stores messages that are pending commit.
///
/// This is used to retreive raw messages in case they need to be placed in the DLQ.
#[derive(Debug, Clone, Default)]
pub struct BufferedMessages<TPayload> {
    max_per_partition: Option<usize>,
    buffered_messages: BTreeMap<Partition, VecDeque<BrokerMessage<TPayload>>>,
}

impl<TPayload> BufferedMessages<TPayload> {
    pub fn new(max_per_partition: Option<usize>) -> Self {
        BufferedMessages {
            max_per_partition,
            buffered_messages: BTreeMap::new(),
        }
    }

    /// Add a message to the buffer.
    ///
    /// If the configured `max_per_partition` is `0`, this is a no-op.
    pub fn append(&mut self, message: BrokerMessage<TPayload>) {
        if self.max_per_partition == Some(0) {
            return;
        }

        let buffered = self.buffered_messages.entry(message.partition).or_default();
        if let Some(max) = self.max_per_partition {
            if buffered.len() >= max {
                tracing::warn!(
                    "DLQ buffer exceeded, dropping message on partition {}",
                    message.partition.index
                );
                buffered.pop_front();
            }
        }

        buffered.push_back(message);

        gauge!(
            "Number of elements in buffer deque without reallocating",
            buffered.capacity()
        );
    }

    /// Return the message at the given offset or None if it is not found in the buffer.
    /// Messages up to the offset for the given partition are removed.
    pub fn pop(&mut self, partition: &Partition, offset: u64) -> Option<BrokerMessage<TPayload>> {
        let messages = self.buffered_messages.get_mut(partition)?;
        while let Some(message) = messages.front() {
            match message.offset.cmp(&offset) {
                Ordering::Equal => {
                    return messages.pop_front();
                }
                Ordering::Greater => {
                    return None;
                }
                Ordering::Less => {
                    messages.pop_front();
                }
            };
        }

        None
    }

    // Clear the buffer. Should be called on rebalance.
    pub fn reset(&mut self) {
        self.buffered_messages.clear();
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use std::sync::Mutex;

    use chrono::Utc;

    use crate::processing::strategies::run_task_in_threads::ConcurrencyConfig;
    use crate::types::Topic;

    #[test]
    fn test_buffered_messages() {
        let mut buffer = BufferedMessages::new(None);
        let partition = Partition {
            topic: Topic::new("test"),
            index: 1,
        };

        for i in 0..10 {
            buffer.append(BrokerMessage {
                partition,
                offset: i,
                payload: i,
                timestamp: Utc::now(),
            });
        }

        assert_eq!(buffer.pop(&partition, 0).unwrap().offset, 0);
        assert_eq!(buffer.pop(&partition, 8).unwrap().offset, 8);
        assert!(buffer.pop(&partition, 1).is_none()); // Removed when we popped offset 8
        assert_eq!(buffer.pop(&partition, 9).unwrap().offset, 9);
        assert!(buffer.pop(&partition, 10).is_none()); // Doesn't exist
    }

    #[test]
    fn test_buffered_messages_limit() {
        let mut buffer = BufferedMessages::new(Some(2));
        let partition = Partition {
            topic: Topic::new("test"),
            index: 1,
        };

        for i in 0..10 {
            buffer.append(BrokerMessage {
                partition,
                offset: i,
                payload: i,
                timestamp: Utc::now(),
            });
        }

        // It's gone
        assert!(buffer.pop(&partition, 1).is_none());

        assert_eq!(buffer.pop(&partition, 9).unwrap().payload, 9);
    }

    #[test]
    fn test_no_buffered_messages() {
        let mut buffer = BufferedMessages::new(Some(0));
        let partition = Partition {
            topic: Topic::new("test"),
            index: 1,
        };

        for i in 0..10 {
            buffer.append(BrokerMessage {
                partition,
                offset: i,
                payload: i,
                timestamp: Utc::now(),
            });
        }

        assert!(buffer.pop(&partition, 9).is_none());
    }

    #[derive(Clone)]
    struct TestDlqProducer {
        pub call_count: Arc<Mutex<usize>>,
    }

    impl TestDlqProducer {
        fn new() -> Self {
            TestDlqProducer {
                call_count: Arc::new(Mutex::new(0)),
            }
        }
    }

    impl<TPayload: Send + Sync + 'static> DlqProducer<TPayload> for TestDlqProducer {
        fn produce(
            &self,
            message: BrokerMessage<TPayload>,
        ) -> Pin<Box<dyn Future<Output = BrokerMessage<TPayload>> + Send + Sync>> {
            *self.call_count.lock().unwrap() += 1;
            Box::pin(async move { message })
        }

        fn build_initial_state(
            &self,
            limit: DlqLimit,
            assignment: &HashMap<Partition, u64>,
        ) -> DlqLimitState {
            DlqLimitState::new(
                limit,
                assignment
                    .iter()
                    .map(|(p, _)| (*p, InvalidMessageStats::default()))
                    .collect(),
            )
        }
    }

    #[test]
    fn test_dlq_policy_wrapper() {
        let partition = Partition {
            topic: Topic::new("test"),
            index: 1,
        };

        let producer = TestDlqProducer::new();

        let handle = ConcurrencyConfig::new(10).handle();
        let mut wrapper = DlqPolicyWrapper::new(Some(DlqPolicy::new(
            handle,
            Box::new(producer.clone()),
            DlqLimit::default(),
            None,
        )));

        wrapper.reset_dlq_limits(&HashMap::from([(partition, 0)]));

        for i in 0..10 {
            wrapper.produce(BrokerMessage {
                partition,
                offset: i,
                payload: i,
                timestamp: Utc::now(),
            });
        }

        wrapper.flush(&HashMap::from([(partition, 11)]));

        assert_eq!(*producer.call_count.lock().unwrap(), 10);
    }

    #[test]
    #[should_panic]
    fn test_dlq_policy_wrapper_limit_exceeded() {
        let partition = Partition {
            topic: Topic::new("test"),
            index: 1,
        };

        let producer = TestDlqProducer::new();

        let handle = ConcurrencyConfig::new(10).handle();
        let mut wrapper = DlqPolicyWrapper::new(Some(DlqPolicy::new(
            handle,
            Box::new(producer),
            DlqLimit {
                max_consecutive_count: Some(5),
                ..Default::default()
            },
            None,
        )));

        wrapper.reset_dlq_limits(&HashMap::from([(partition, 0)]));

        for i in 0..10 {
            wrapper.produce(BrokerMessage {
                partition,
                offset: i,
                payload: i,
                timestamp: Utc::now(),
            });
        }

        wrapper.flush(&HashMap::from([(partition, 11)]));
    }

    #[test]
    fn test_dlq_limit_state() {
        let partition = Partition::new(Topic::new("test_topic"), 0);
        let limit = DlqLimit {
            max_consecutive_count: Some(5),
            ..Default::default()
        };

        let mut state = DlqLimitState::new(
            limit,
            HashMap::from([(partition, InvalidMessageStats::invalid_at(3))]),
        );

        // 1 valid message followed by 4 invalid
        for i in 4..9 {
            let msg = BrokerMessage::new(i, partition, i, chrono::Utc::now());
            assert!(state.record_invalid_message(&msg));
        }

        // Next message should not be accepted
        let msg = BrokerMessage::new(9, partition, 9, chrono::Utc::now());
        assert!(!state.record_invalid_message(&msg));
    }
}
