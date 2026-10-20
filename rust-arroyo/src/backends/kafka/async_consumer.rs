use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use parking_lot::Mutex;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::Statistics;
use sentry_core::Hub;
use tokio::sync::Notify;

use super::config::KafkaConfig;
use super::types::KafkaPayload;
use super::{
    create_kafka_message, kafka_poll_error_is_recoverable, log_librdkafka, log_librdkafka_error,
    record_consumer_stats,
};
use crate::types::{BrokerMessage, Partition, Topic};

const POLL_TIMEOUT: Duration = Duration::from_millis(100);

/// Callbacks run on the consumer's poll thread and may block. librdkafka applies each
/// rebalance only after `on_assign` or `on_revoke` returns.
pub trait AsyncAssignmentCallbacks: Send + Sync + 'static {
    fn on_assign(&self, queues: Vec<AsyncPartitionQueue>);

    /// Finish processing these partitions before returning; their queues are closed
    /// immediately afterward. With `enable.auto.offset.store=false`, store their final
    /// offsets with [`AsyncKafkaConsumer::store_offsets`] before returning.
    /// Shutdown always commits stored offsets synchronously after this callback returns.
    fn on_revoke(&self, partitions: Vec<Partition>);

    /// Consumer errors other than broker transport failures, which are logged either way.
    fn on_error(&self, _error: KafkaError) {}
}

/// A Kafka consumer that gives each assigned partition its own [`AsyncPartitionQueue`]. Its
/// own thread polls the consumer and serves rebalances through [`AsyncAssignmentCallbacks`].
/// Dropping it blocks until revocation and the final commit finish: drop it in
/// `spawn_blocking`, never from a thread that `on_revoke` waits on.
pub struct AsyncKafkaConsumer {
    consumer: Arc<BaseConsumer<AsyncConsumerContext>>,
    shutdown: Arc<AtomicBool>,
    poll_thread: Option<JoinHandle<()>>,
}

impl AsyncKafkaConsumer {
    pub fn new(
        config: KafkaConfig,
        topics: &[Topic],
        callbacks: impl AsyncAssignmentCallbacks,
    ) -> Result<Self, KafkaError> {
        // Strict reset needs missing offsets resolved before assignment, like KafkaConsumer does.
        if config
            .offset_reset_config()
            .is_some_and(|reset| reset.strict_offset_reset)
        {
            return Err(KafkaError::ClientCreation(
                "AsyncKafkaConsumer does not support strict_offset_reset".to_owned(),
            ));
        }
        let context = AsyncConsumerContext {
            hub: Hub::current(),
            callbacks: Box::new(callbacks),
            consumer: OnceLock::new(),
            queues: Mutex::default(),
        };
        let mut config: ClientConfig = config.into();
        config.set_log_level(RDKafkaLogLevel::Warning);
        let consumer: BaseConsumer<AsyncConsumerContext> = config.create_with_context(context)?;
        let consumer = Arc::new(consumer);
        let _ = consumer.context().consumer.set(Arc::downgrade(&consumer));

        let topics: Vec<&str> = topics.iter().map(|topic| topic.as_str()).collect();
        consumer.subscribe(&topics)?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let poll_thread = std::thread::Builder::new()
            .name("arroyo-consumer".to_owned())
            .spawn({
                let consumer = consumer.clone();
                let shutdown = shutdown.clone();
                move || run_poll_thread(&consumer, &shutdown)
            })
            .map_err(|error| {
                KafkaError::ClientCreation(format!(
                    "failed to spawn the consumer poll thread: {error}"
                ))
            })?;

        Ok(Self {
            consumer,
            shutdown,
            poll_thread: Some(poll_thread),
        })
    }

    /// Store processed offsets locally for a later commit. Requires
    /// `enable.auto.offset.store=false` in the consumer configuration.
    pub fn store_offsets(&self, offsets: HashMap<Partition, u64>) -> Result<(), KafkaError> {
        let mut tpl = TopicPartitionList::with_capacity(offsets.len());
        for (partition, offset) in offsets {
            tpl.add_partition_offset(
                partition.topic.as_str(),
                partition.index as i32,
                Offset::Offset(offset as i64),
            )?;
        }
        self.consumer.store_offsets(&tpl)
    }

    /// Synchronously commit the locally stored offsets for the current assignment to Kafka.
    /// With `enable.auto.commit=false`, call this periodically to save progress and
    /// before returning from [`AsyncAssignmentCallbacks::on_revoke`].
    /// Use `spawn_blocking` when calling from an async task.
    pub fn commit_consumer_state(&self) -> Result<(), KafkaError> {
        self.consumer.commit_consumer_state(CommitMode::Sync)
    }
}

impl Drop for AsyncKafkaConsumer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(poll_thread) = self.poll_thread.take() {
            let _ = poll_thread.join();
        }
    }
}

/// A queue wrapper for a single partition.
/// Queues are created when the partition is assigned, and closed when the partition is revoked.
pub struct AsyncPartitionQueue {
    partition: Partition,
    state: Arc<QueueState>,
}

impl AsyncPartitionQueue {
    pub fn partition(&self) -> Partition {
        self.partition
    }

    /// Returns `None` once the partition has been revoked. This is cancellation safe.
    pub async fn recv(&self) -> Option<Result<BrokerMessage<KafkaPayload>, KafkaError>> {
        loop {
            // Created before polling so that a message arriving in between still wakes us.
            let notified = self.state.notify.notified();
            {
                let queue = self.state.queue.lock();
                if let Some(message) = queue.as_ref()?.poll(Duration::ZERO) {
                    return Some(
                        message
                            .map(|message| create_kafka_message(&[self.partition.topic], message)),
                    );
                }
            }
            notified.await;
        }
    }
}

/// The partition's native queue, taken out when the partition is revoked, and a `Notify`
/// that wakes readers in `recv` when a message arrives or the queue is closed.
struct QueueState {
    queue: Mutex<Option<PartitionQueue<AsyncConsumerContext>>>,
    notify: Arc<Notify>,
}

impl QueueState {
    fn new(mut queue: PartitionQueue<AsyncConsumerContext>) -> Arc<Self> {
        let notify = Arc::new(Notify::new());
        queue.set_nonempty_callback({
            let notify = notify.clone();
            move || notify.notify_waiters()
        });
        Arc::new(Self {
            queue: Mutex::new(Some(queue)),
            notify,
        })
    }

    fn close(&self) {
        // Drop the native queue before the partition can be assigned again: dropping a queue
        // disables the wakeup callback of every handle to that partition.
        self.queue.lock().take();
        self.notify.notify_waiters();
    }
}

struct AsyncConsumerContext {
    hub: Arc<Hub>,
    callbacks: Box<dyn AsyncAssignmentCallbacks>,
    /// Rebalances only get `&BaseConsumer`, but splitting a partition queue needs the `Arc`.
    consumer: OnceLock<Weak<BaseConsumer<AsyncConsumerContext>>>,
    queues: Mutex<HashMap<Partition, Arc<QueueState>>>,
}

impl ClientContext for AsyncConsumerContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        log_librdkafka(&self.hub, level, fac, log_message);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        log_librdkafka_error(&self.hub, error, reason);
    }

    fn stats(&self, stats: Statistics) {
        record_consumer_stats(stats);
    }
}

impl ConsumerContext for AsyncConsumerContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(tpl) => self.assign(tpl),
            Rebalance::Revoke(tpl) => self.revoke(tpl),
            // librdkafka only sends assignments and revocations, and rdkafka logs anything else.
            Rebalance::Error(_) => {}
        }
    }
}

impl AsyncConsumerContext {
    fn assign(&self, tpl: &TopicPartitionList) {
        // The consumer is being dropped, so nothing would read these partitions.
        let Some(consumer) = self.consumer.get().and_then(Weak::upgrade) else {
            return;
        };
        let mut assigned = Vec::with_capacity(tpl.count());
        for element in tpl.elements() {
            let partition = to_partition(element.topic(), element.partition());
            let Some(queue) = consumer.split_partition_queue(element.topic(), element.partition())
            else {
                tracing::error!(%partition, "Failed to split partition queue");
                self.callbacks
                    .on_error(KafkaError::Rebalance(RDKafkaErrorCode::UnknownPartition));
                continue;
            };
            let state = QueueState::new(queue);
            self.queues.lock().insert(partition, state.clone());
            assigned.push(AsyncPartitionQueue { partition, state });
        }
        if !assigned.is_empty() {
            self.callbacks.on_assign(assigned);
        }
    }

    fn revoke(&self, tpl: &TopicPartitionList) {
        let revoked: Vec<(Partition, Arc<QueueState>)> = {
            let mut queues = self.queues.lock();
            tpl.elements()
                .iter()
                .filter_map(|element| {
                    let partition = to_partition(element.topic(), element.partition());
                    queues.remove(&partition).map(|state| (partition, state))
                })
                .collect()
        };
        self.revoke_queues(revoked);
    }

    fn revoke_queues(&self, revoked: Vec<(Partition, Arc<QueueState>)>) {
        if revoked.is_empty() {
            return;
        }
        self.callbacks
            .on_revoke(revoked.iter().map(|(partition, _)| *partition).collect());
        for (_, state) in revoked {
            state.close();
        }
    }
}

fn to_partition(topic: &str, partition: i32) -> Partition {
    Partition::new(Topic::new(topic), partition as u16)
}

fn run_poll_thread(consumer: &BaseConsumer<AsyncConsumerContext>, shutdown: &AtomicBool) {
    while !shutdown.load(Ordering::Relaxed) {
        poll_once(consumer);
    }
    let context = consumer.context();
    let remaining = context.queues.lock().drain().collect();
    context.revoke_queues(remaining);
    match consumer.commit_consumer_state(CommitMode::Sync) {
        Ok(()) | Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset)) => {}
        Err(error) => {
            tracing::error!(%error, "Failed to commit Kafka offsets during shutdown");
            context.callbacks.on_error(error);
        }
    }
}

fn poll_once(consumer: &BaseConsumer<AsyncConsumerContext>) {
    match consumer.poll(POLL_TIMEOUT) {
        None => {}
        // Every assigned partition has its own queue, so this should never happen.
        Some(Ok(message)) => {
            tracing::error!(
                topic = message.topic(),
                partition = message.partition(),
                offset = message.offset(),
                "Dropping a message received outside of its partition queue"
            );
        }
        Some(Err(error)) if kafka_poll_error_is_recoverable(&error) => {
            tracing::warn!(%error, "Kafka poll transport error, retrying...");
        }
        Some(Err(error)) => {
            tracing::error!(%error, "Kafka consumer error");
            consumer.context().callbacks.on_error(error);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use rdkafka::error::KafkaError;
    use tokio::runtime::Runtime;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    use tokio::time::timeout;

    use super::{AsyncAssignmentCallbacks, AsyncKafkaConsumer, AsyncPartitionQueue};
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::producer::KafkaProducer;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::backends::kafka::InitialOffset;
    use crate::backends::Producer;
    use crate::testutils::{get_default_broker, TestTopic};
    use crate::types::{Partition, Topic, TopicOrPartition};

    const TIMEOUT: Duration = Duration::from_secs(30);

    struct ChannelCallbacks {
        assigned: UnboundedSender<Vec<AsyncPartitionQueue>>,
        revoked: UnboundedSender<Vec<Partition>>,
    }

    impl AsyncAssignmentCallbacks for ChannelCallbacks {
        fn on_assign(&self, queues: Vec<AsyncPartitionQueue>) {
            self.assigned.send(queues).unwrap();
        }

        fn on_revoke(&self, partitions: Vec<Partition>) {
            self.revoked.send(partitions).unwrap();
        }
    }

    struct TestConsumer {
        consumer: AsyncKafkaConsumer,
        assigned: UnboundedReceiver<Vec<AsyncPartitionQueue>>,
        revoked: UnboundedReceiver<Vec<Partition>>,
    }

    impl TestConsumer {
        fn new(topic: Topic, group_id: &str) -> Self {
            let config = KafkaConfig::new_consumer_config(
                vec![get_default_broker()],
                group_id.to_owned(),
                InitialOffset::Earliest,
                false,
                30_000,
                Some(HashMap::from([
                    ("enable.auto.commit".to_owned(), "true".to_owned()),
                    ("enable.auto.offset.store".to_owned(), "false".to_owned()),
                ])),
            );
            let (assigned_sender, assigned) = unbounded_channel();
            let (revoked_sender, revoked) = unbounded_channel();
            let callbacks = ChannelCallbacks {
                assigned: assigned_sender,
                revoked: revoked_sender,
            };
            Self {
                consumer: AsyncKafkaConsumer::new(config, &[topic], callbacks).unwrap(),
                assigned,
                revoked,
            }
        }

        async fn assignment(&mut self) -> Vec<AsyncPartitionQueue> {
            let mut queues = next(&mut self.assigned).await;
            queues.sort_by_key(|queue| queue.partition().index);
            queues
        }

        async fn shutdown(mut self) -> Vec<Partition> {
            let consumer = self.consumer;
            tokio::task::spawn_blocking(move || drop(consumer))
                .await
                .unwrap();
            let mut revoked = next(&mut self.revoked).await;
            revoked.sort_by_key(|partition| partition.index);
            revoked
        }
    }

    async fn next<T>(receiver: &mut UnboundedReceiver<T>) -> T {
        timeout(TIMEOUT, receiver.recv()).await.unwrap().unwrap()
    }

    fn produce(messages: &[(Partition, &str)]) {
        let config = KafkaConfig::new_producer_config(vec![get_default_broker()], None);
        let producer = KafkaProducer::new(config).unwrap();
        for (partition, payload) in messages {
            let payload = KafkaPayload::new(None, None, Some(payload.as_bytes().to_vec()));
            producer
                .produce(&TopicOrPartition::Partition(*partition), payload)
                .unwrap();
        }
        producer.flush_for(TIMEOUT).unwrap();
    }

    async fn recv_payload(queue: &AsyncPartitionQueue) -> (u64, String) {
        let message = timeout(TIMEOUT, queue.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(message.partition, queue.partition());
        let payload = message.payload.payload().unwrap().clone();
        (message.offset, String::from_utf8(payload).unwrap())
    }

    async fn recv_closed(queue: &AsyncPartitionQueue) -> bool {
        timeout(TIMEOUT, queue.recv()).await.unwrap().is_none()
    }

    fn group_id() -> String {
        format!("test-async-consumer-{}", uuid::Uuid::new_v4())
    }

    fn runtime() -> Runtime {
        // Callbacks run on the consumer's own thread, so one runtime thread is enough.
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn test_each_partition_has_its_own_queue() {
        let topic = TestTopic::create_with_partitions("async-consumer-queues", 2);
        let partitions = [0, 1].map(|index| Partition::new(topic.topic, index));
        produce(&[
            (partitions[0], "a"),
            (partitions[1], "b"),
            (partitions[0], "c"),
        ]);

        runtime().block_on(async {
            let mut consumer = TestConsumer::new(topic.topic, &group_id());
            let queues = consumer.assignment().await;
            let assigned: Vec<_> = queues.iter().map(|queue| queue.partition()).collect();
            assert_eq!(assigned, partitions);

            assert_eq!(recv_payload(&queues[0]).await, (0, "a".to_owned()));
            assert_eq!(recv_payload(&queues[0]).await, (1, "c".to_owned()));
            assert_eq!(recv_payload(&queues[1]).await, (0, "b".to_owned()));

            assert_eq!(consumer.shutdown().await, partitions);
            assert!(recv_closed(&queues[0]).await);
            assert!(recv_closed(&queues[1]).await);
        });
    }

    #[test]
    fn test_stored_offsets_are_committed_on_shutdown() {
        let topic = TestTopic::create("async-consumer-commit");
        let partition = Partition::new(topic.topic, 0);
        produce(&[(partition, "0"), (partition, "1"), (partition, "2")]);
        let group_id = group_id();

        runtime().block_on(async {
            let mut consumer = TestConsumer::new(topic.topic, &group_id);
            let queues = consumer.assignment().await;
            for expected in 0..3 {
                assert_eq!(recv_payload(&queues[0]).await.0, expected);
            }
            // Only the first two messages are done.
            consumer
                .consumer
                .store_offsets(HashMap::from([(partition, 2)]))
                .unwrap();
            assert_eq!(consumer.shutdown().await, [partition]);

            let mut consumer = TestConsumer::new(topic.topic, &group_id);
            let queues = consumer.assignment().await;
            assert_eq!(recv_payload(&queues[0]).await, (2, "2".to_owned()));
            consumer.shutdown().await;
        });
    }

    #[test]
    fn test_rebalance_splits_fresh_queues() {
        let topic = TestTopic::create_with_partitions("async-consumer-rebalance", 2);
        let group_id = group_id();

        runtime().block_on(async {
            let mut first = TestConsumer::new(topic.topic, &group_id);
            let old_queues = first.assignment().await;
            assert_eq!(old_queues.len(), 2);

            let mut second = TestConsumer::new(topic.topic, &group_id);
            assert_eq!(next(&mut first.revoked).await.len(), 2);
            assert!(recv_closed(&old_queues[0]).await);

            let first_queues = first.assignment().await;
            let second_queues = second.assignment().await;
            assert_eq!(first_queues.len(), 1);
            assert_eq!(second_queues.len(), 1);
            // Like a worker that lets go of its queue late, after the partition was split again.
            drop(old_queues);

            produce(&[
                (first_queues[0].partition(), "after"),
                (second_queues[0].partition(), "after"),
            ]);
            assert_eq!(
                recv_payload(&first_queues[0]).await,
                (0, "after".to_owned())
            );
            assert_eq!(
                recv_payload(&second_queues[0]).await,
                (0, "after".to_owned())
            );

            first.shutdown().await;
            second.shutdown().await;
        });
    }

    #[test]
    fn test_strict_offset_reset_is_rejected() {
        let config = KafkaConfig::new_consumer_config(
            vec![get_default_broker()],
            group_id(),
            InitialOffset::Earliest,
            true,
            30_000,
            None,
        );
        let (assigned, _) = unbounded_channel();
        let (revoked, _) = unbounded_channel();
        let callbacks = ChannelCallbacks { assigned, revoked };
        let result = AsyncKafkaConsumer::new(config, &[Topic::new("test")], callbacks);
        assert!(matches!(result, Err(KafkaError::ClientCreation(_))));
    }

    #[allow(dead_code)]
    fn recv_is_send(queue: &AsyncPartitionQueue) {
        fn assert_send<T: Send>(_: T) {}
        assert_send(queue.recv());
    }
}
