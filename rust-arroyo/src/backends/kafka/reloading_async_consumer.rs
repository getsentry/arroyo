//! An [`AsyncKafkaConsumer`] that moves to another topic or cluster when config changes.
//!
//! # Swapping consumers
//!
//! A pushed config is probed first: a client built from it has to reach a broker and find
//! the topic. Until then the running consumer keeps going, and the probe is retried every
//! [`ReloadConfig::probe_retry_interval`].
//!
//! Once the probe passes, the old consumer revokes its partitions through
//! [`AsyncAssignmentCallbacks::on_revoke`], commits the offsets stored before it returns, and
//! leaves the group. A consumer for the new config then joins, and its partitions arrive
//! through [`AsyncAssignmentCallbacks::on_assign`]. To the host, a swap looks like a
//! rebalance.
//!
//! # Starting offsets
//!
//! A consumer started by a swap uses `auto.offset.reset=earliest`. The group has no committed
//! offsets on the new topic yet, and producers cut over first, so `latest` would skip what
//! they already wrote. A consumer started by [`ReloadingAsyncKafkaConsumer::new`] uses the
//! blob's setting, so a cutover has to push `earliest` too, for pods that start mid-cutover.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Condvar, Mutex, RwLock};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use super::async_consumer::{AsyncAssignmentCallbacks, AsyncKafkaConsumer};
use super::config::KafkaConfig;
use super::config_blob::{ConfigBlob, ConsumerSelector, ResolvedClientConfig};
use super::reload::{
    blob_error_reason, config_eq, superseded, ReloadConfig, ReloadError, ReloadOutcome, ReloadState,
};
use crate::metrics;
use crate::types::{Partition, Topic};

/// An [`AsyncKafkaConsumer`] whose config can be replaced while it runs.
///
/// Only [`ReloadConfig::jitter`] and the probe settings apply to consumers.
///
/// Dropping it blocks until revocation and the final commit finish: drop it in
/// `spawn_blocking`, never from a thread that `on_revoke` waits on.
pub struct ReloadingAsyncKafkaConsumer {
    inner: Arc<Inner>,
}

struct Inner {
    current: RwLock<Current>,
    callbacks: Arc<dyn AsyncAssignmentCallbacks>,
    /// Serializes reloads and holds the state a swap needs.
    reload: Mutex<ReloadState>,
    /// Signalled when `reload.desired` or `reload.shutdown` changes.
    reload_wake: Condvar,
    /// Held for a whole swap, so a drop waits for it rather than racing it.
    swapping: Mutex<()>,
    selector: ConsumerSelector,
    settings: ReloadConfig,
    generation: AtomicU64,
    /// Test-only probe attempt count.
    #[cfg(test)]
    probe_attempts: AtomicU64,
}

struct Current {
    consumer: Option<Arc<AsyncKafkaConsumer>>,
    config: ResolvedClientConfig,
}

impl ReloadingAsyncKafkaConsumer {
    /// Starts a consumer from an initial config blob.
    pub fn new(
        blob: &[u8],
        selector: ConsumerSelector,
        settings: ReloadConfig,
        callbacks: impl AsyncAssignmentCallbacks,
    ) -> Result<Self, ReloadError> {
        let config = match ConfigBlob::parse(blob).and_then(|blob| blob.consumer_config(&selector))
        {
            Ok(config) => config,
            Err(error) => {
                metrics::counter!(
                    "arroyo.consumer.config_reload_rejected",
                    "topic" => selector.topic().to_owned(),
                    "consumer_group" => selector.group_id().to_owned(),
                    "source" => "initial",
                    "reason" => blob_error_reason(&error),
                )
                .increment(1);
                return Err(error.into());
            }
        };
        Self::start(config, selector, settings, callbacks)
    }

    /// Starts a consumer from a config the host built itself.
    pub fn from_config(
        config: KafkaConfig,
        selector: ConsumerSelector,
        settings: ReloadConfig,
        callbacks: impl AsyncAssignmentCallbacks,
    ) -> Result<Self, ReloadError> {
        let config = ResolvedClientConfig {
            config,
            physical_topic: Topic::new(selector.topic()),
        };
        Self::start(config, selector, settings, callbacks)
    }

    fn start(
        config: ResolvedClientConfig,
        selector: ConsumerSelector,
        settings: ReloadConfig,
        callbacks: impl AsyncAssignmentCallbacks,
    ) -> Result<Self, ReloadError> {
        let callbacks: Arc<dyn AsyncAssignmentCallbacks> = Arc::new(callbacks);
        let consumer = AsyncKafkaConsumer::new(
            config.config.clone(),
            &[config.physical_topic],
            callbacks.clone(),
        )?;
        tracing::info!(
            topic = %selector.topic(),
            physical_topic = %config.physical_topic,
            app = %selector.app(),
            consumer_group = %selector.group_id(),
            "loaded initial consumer config"
        );
        metrics::counter!(
            "arroyo.consumer.config_reload_loaded",
            "topic" => selector.topic().to_owned(),
            "consumer_group" => selector.group_id().to_owned(),
        )
        .increment(1);
        // Report the gauges up front, so consumers that never reload still show up.
        metrics::gauge!(
            "arroyo.consumer.config_reload_generation",
            "topic" => selector.topic().to_owned(),
            "consumer_group" => selector.group_id().to_owned(),
        )
        .set(0.0);
        metrics::gauge!(
            "arroyo.consumer.config_reload_pending",
            "topic" => selector.topic().to_owned(),
            "consumer_group" => selector.group_id().to_owned(),
        )
        .set(0.0);

        let inner = Arc::new(Inner {
            current: RwLock::new(Current {
                consumer: Some(Arc::new(consumer)),
                config,
            }),
            callbacks,
            reload: Mutex::new(ReloadState {
                desired: None,
                desired_since: None,
                shutdown: false,
            }),
            reload_wake: Condvar::new(),
            swapping: Mutex::new(()),
            selector,
            settings,
            generation: AtomicU64::new(0),
            #[cfg(test)]
            probe_attempts: AtomicU64::new(0),
        });

        let worker = inner.clone();
        std::thread::Builder::new()
            .name("arroyo-consumer-reload".to_owned())
            .spawn(move || worker.run_worker())
            .map_err(ReloadError::WorkerSpawn)?;

        Ok(Self { inner })
    }

    /// Hands a new config blob to the consumer.
    pub fn push_config(&self, blob: &[u8]) -> Result<ReloadOutcome, ReloadError> {
        let parsed =
            ConfigBlob::parse(blob).and_then(|blob| blob.consumer_config(&self.inner.selector));

        match parsed {
            Ok(config) => Ok(self.inner.set_desired(config)),
            Err(error) => {
                metrics::counter!(
                    "arroyo.consumer.config_reload_rejected",
                    "topic" => self.inner.selector.topic().to_owned(),
                    "consumer_group" => self.inner.selector.group_id().to_owned(),
                    "source" => "push",
                    "reason" => blob_error_reason(&error),
                )
                .increment(1);
                tracing::error!(
                    topic = %self.inner.selector.topic(),
                    %error,
                    "could not resolve the pushed config blob, staying on the running config"
                );
                Err(error.into())
            }
        }
    }

    /// Number of swaps so far.
    pub fn generation(&self) -> u64 {
        self.inner.generation.load(Ordering::SeqCst)
    }

    /// See [`AsyncKafkaConsumer::store_offsets`]
    /// Offsets stored in [`AsyncAssignmentCallbacks::on_revoke`] during a swap
    /// go to the old consumer, which commits them before it leaves.
    pub fn store_offsets(&self, offsets: HashMap<Partition, u64>) -> Result<(), KafkaError> {
        match &self.inner.current.read().consumer {
            Some(consumer) => consumer.store_offsets(offsets),
            None => Err(KafkaError::StoreOffset(RDKafkaErrorCode::State)),
        }
    }

    /// See [`AsyncKafkaConsumer::commit_consumer_state`].
    pub fn commit_consumer_state(&self) -> Result<(), KafkaError> {
        match &self.inner.current.read().consumer {
            Some(consumer) => consumer.commit_consumer_state(),
            None => Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::State)),
        }
    }
}

impl Drop for ReloadingAsyncKafkaConsumer {
    fn drop(&mut self) {
        self.inner.reload.lock().shutdown = true;
        self.inner.reload_wake.notify_all();

        // A swap in flight sees `shutdown` and leaves no new consumer behind.
        let _swapping = self.inner.swapping.lock();
        let consumer = self.inner.current.read().consumer.clone();
        if let Some(consumer) = consumer {
            // Stopped while still installed, so `on_revoke` can store final offsets.
            consumer.stop();
            self.inner.current.write().consumer = None;
        }
    }
}

impl Inner {
    /// Records whether a rollout is pending. Call under `reload` after each change to
    /// `desired` so the gauge cannot drift.
    fn record_pending(&self, reload: &ReloadState) {
        metrics::gauge!(
            "arroyo.consumer.config_reload_pending",
            "topic" => self.selector.topic().to_owned(),
            "consumer_group" => self.selector.group_id().to_owned(),
        )
        .set(u8::from(reload.desired.is_some()));
    }

    /// Records the config the host wants running and wakes the worker.
    fn set_desired(&self, config: ResolvedClientConfig) -> ReloadOutcome {
        let mut reload = self.reload.lock();

        // Check if the config is the same as the current one and the consumer is running.
        let current = self.current.read();
        if current.consumer.is_some() && config_eq(&config, &current.config) {
            reload.desired = None;
            reload.desired_since = None;
            self.reload_wake.notify_all();
            self.record_pending(&reload);
            metrics::counter!(
                "arroyo.consumer.config_reload_unchanged",
                "topic" => self.selector.topic().to_owned(),
                "consumer_group" => self.selector.group_id().to_owned(),
            )
            .increment(1);
            return ReloadOutcome::Unchanged;
        }

        reload.desired = Some(config);
        reload.desired_since = Some(std::time::Instant::now());
        self.reload_wake.notify_all();
        self.record_pending(&reload);
        metrics::counter!(
            "arroyo.consumer.config_reload_accepted",
            "topic" => self.selector.topic().to_owned(),
            "consumer_group" => self.selector.group_id().to_owned(),
        )
        .increment(1);
        ReloadOutcome::Accepted
    }

    /// Runs broker probes and swaps until the consumer is dropped.
    fn run_worker(&self) {
        loop {
            let config = {
                let mut reload = self.reload.lock();
                loop {
                    if reload.shutdown {
                        return;
                    }
                    // Clone rather than take: a newer push or revert can supersede this rollout.
                    match reload.desired.clone() {
                        Some(config) => break config,
                        None => self.reload_wake.wait(&mut reload),
                    }
                }
            };

            self.reload_to(&config);

            let mut reload = self.reload.lock();
            if reload
                .desired
                .as_ref()
                .is_some_and(|desired| config_eq(desired, &config))
            {
                reload.desired = None;
                reload.desired_since = None;
            }
            self.record_pending(&reload);
        }
    }

    /// Probes a config and swaps to it
    fn reload_to(&self, config: &ResolvedClientConfig) {
        // Built now but subscribed only once the old consumer has left, so a failure here
        // leaves the old one running.
        let candidate =
            match AsyncKafkaConsumer::build(earliest_reset(&config.config), self.callbacks.clone())
            {
                Ok(candidate) => candidate,
                Err(error) => {
                    metrics::counter!(
                        "arroyo.consumer.config_reload_rejected",
                        "topic" => self.selector.topic().to_owned(),
                        "consumer_group" => self.selector.group_id().to_owned(),
                        "source" => "push",
                        "reason" => "client_build",
                    )
                    .increment(1);
                    tracing::error!(
                        topic = %self.selector.topic(),
                        %error,
                        "kafka rejected the new config, staying on the running one"
                    );
                    return;
                }
            };

        loop {
            #[cfg(test)]
            self.probe_attempts.fetch_add(1, Ordering::SeqCst);

            match candidate.validate_topic(config.physical_topic, self.settings.probe_timeout) {
                Ok(()) => {
                    self.swap(candidate, config);
                    return;
                }
                Err(error) => {
                    metrics::counter!(
                        "arroyo.consumer.config_reload_probe_failed",
                        "topic" => self.selector.topic().to_owned(),
                        "consumer_group" => self.selector.group_id().to_owned(),
                    )
                    .increment(1);
                    tracing::warn!(
                        topic = %self.selector.topic(),
                        %error,
                        "new config cannot reach its topic, staying on the running one"
                    );
                }
            }

            let mut reload = self.reload.lock();
            if superseded(&reload, config) {
                return;
            }
            self.reload_wake
                .wait_for(&mut reload, self.settings.probe_retry_interval);
            if superseded(&reload, config) {
                return;
            }
        }
    }

    /// Replaces the running consumer with an already-probed one that is not subscribed yet.
    fn swap(&self, candidate: AsyncKafkaConsumer, config: &ResolvedClientConfig) {
        // Spread swaps across instances.
        if !self.settings.jitter.is_zero() {
            let jitter = rand::random::<f64>() * self.settings.jitter.as_secs_f64();
            std::thread::sleep(Duration::from_secs_f64(jitter));
        }

        let _swapping = self.swapping.lock();
        if superseded(&self.reload.lock(), config) {
            return;
        }

        // Stopped while still installed, so `on_revoke` can store final offsets through
        // `store_offsets`, and they get committed.
        let old = self.current.read().consumer.clone();
        if let Some(old) = &old {
            old.stop();
        }
        self.current.write().consumer = None;
        drop(old);

        let reload = self.reload.lock();
        let mut current = self.current.write();

        // While the old consumer stopped, the host may have dropped it or changed its mind.
        // Dropped: start nothing.
        // Superseded by a newer push or a revert: restart the current running config.
        if reload.shutdown {
            return;
        }
        let (target, started) = if superseded(&reload, config) {
            let target = current.config.clone();
            let started = AsyncKafkaConsumer::new(
                earliest_reset(&target.config),
                &[target.physical_topic],
                self.callbacks.clone(),
            );
            (target, started)
        } else {
            let started = candidate
                .subscribe(&[config.physical_topic])
                .map(|()| candidate);
            (config.clone(), started)
        };

        match started {
            Ok(consumer) => {
                current.consumer = Some(Arc::new(consumer));
                if config_eq(&target, &current.config) {
                    return;
                }
                current.config = target;
                let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
                metrics::gauge!(
                    "arroyo.consumer.config_reload_generation",
                    "topic" => self.selector.topic().to_owned(),
                    "consumer_group" => self.selector.group_id().to_owned(),
                )
                .set(generation as f64);
                tracing::info!(
                    topic = %self.selector.topic(),
                    physical_topic = %current.config.physical_topic,
                    generation,
                    "swapped to the new consumer config"
                );
            }
            Err(error) => {
                // The client already exists, or was just built from a running config, so this
                // is unlikely.
                tracing::error!(
                    topic = %self.selector.topic(),
                    %error,
                    "could not start the new consumer, nothing is consuming"
                );
                self.callbacks.on_error(error);
            }
        }
    }
}

/// Starts from the beginning of the topic when no committed offsets are available.
fn earliest_reset(config: &KafkaConfig) -> KafkaConfig {
    let mut params = config.config_map().clone();
    params.insert("auto.offset.reset".to_owned(), "earliest".to_owned());
    KafkaConfig::new_config(Vec::new(), Some(params))
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use tokio::runtime::Runtime;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    use tokio::time::timeout;

    use super::*;
    use crate::backends::kafka::async_consumer::AsyncPartitionQueue;
    use crate::backends::kafka::producer::KafkaProducer;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::backends::Producer;
    use crate::testutils::{get_default_broker, TestTopic};
    use crate::types::{Topic, TopicOrPartition};

    const TIMEOUT: Duration = Duration::from_secs(30);

    /// Revoked partitions, and a sender whose drop lets `on_revoke` return, like Taskbroker's
    /// rendezvous.
    type Revocation = (Vec<Partition>, mpsc::Sender<()>);

    struct ChannelCallbacks {
        assigned: UnboundedSender<Vec<AsyncPartitionQueue>>,
        revoked: UnboundedSender<Revocation>,
    }

    impl AsyncAssignmentCallbacks for ChannelCallbacks {
        fn on_assign(&self, queues: Vec<AsyncPartitionQueue>) {
            let _ = self.assigned.send(queues);
        }

        fn on_revoke(&self, partitions: Vec<Partition>) {
            let (done, wait) = mpsc::channel();
            if self.revoked.send((partitions, done)).is_ok() {
                let _ = wait.recv();
            }
        }
    }

    struct TestConsumer {
        // Dropped before `consumer`, so a failing test does not hang in `on_revoke`.
        revoked: UnboundedReceiver<Revocation>,
        assigned: UnboundedReceiver<Vec<AsyncPartitionQueue>>,
        consumer: ReloadingAsyncKafkaConsumer,
    }

    impl TestConsumer {
        fn new(blob: &[u8]) -> Self {
            let (assigned_sender, assigned) = unbounded_channel();
            let (revoked_sender, revoked) = unbounded_channel();
            let callbacks = ChannelCallbacks {
                assigned: assigned_sender,
                revoked: revoked_sender,
            };
            let selector = ConsumerSelector::new(
                "ingest-events",
                "test",
                format!("test-reloading-consumer-{}", uuid::Uuid::new_v4()),
            );
            let settings = ReloadConfig {
                probe_timeout: Duration::from_secs(2),
                probe_retry_interval: Duration::from_millis(50),
                ..ReloadConfig::default()
            };
            Self {
                consumer: ReloadingAsyncKafkaConsumer::new(blob, selector, settings, callbacks)
                    .unwrap(),
                assigned,
                revoked,
            }
        }

        async fn assignment(&mut self) -> Vec<AsyncPartitionQueue> {
            next(&mut self.assigned).await
        }

        async fn revocation(&mut self) -> Revocation {
            next(&mut self.revoked).await
        }

        async fn shutdown(mut self) -> Vec<Partition> {
            let consumer = self.consumer;
            let dropped = tokio::task::spawn_blocking(move || drop(consumer));
            let (revoked, _) = next(&mut self.revoked).await;
            dropped.await.unwrap();
            revoked
        }
    }

    fn blob(servers: &str, topic: Topic) -> Vec<u8> {
        format!(
            r#"{{
                "clusters": {{"events": {{"config": {{"bootstrap.servers": "{servers}"}}}}}},
                "topics": {{
                    "ingest-events": {{
                        "physical_topics": [{{"cluster": "events", "topic_name_override": "{topic}"}}]
                    }}
                }}
            }}"#
        )
        .into_bytes()
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
        let payload = message.payload.payload().unwrap().clone();
        (message.offset, String::from_utf8(payload).unwrap())
    }

    fn runtime() -> Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn test_swap_moves_to_the_new_topic_and_back() {
        let old_topic = TestTopic::create("reloading-consumer-old");
        let new_topic = TestTopic::create("reloading-consumer-new");
        let old = Partition::new(old_topic.topic, 0);
        let new = Partition::new(new_topic.topic, 0);
        // Written before the swap, like producers that cut over first. The initial consumer
        // starts from `latest`, so it never reads `old-0`.
        produce(&[(old, "old-0"), (new, "new-0")]);
        let broker = get_default_broker();

        runtime().block_on(async {
            let mut consumer = TestConsumer::new(&blob(&broker, old_topic.topic));
            consumer.assignment().await;

            let outcome = consumer
                .consumer
                .push_config(&blob(&broker, new_topic.topic))
                .unwrap();
            assert_eq!(outcome, ReloadOutcome::Accepted);
            // Like Taskbroker, store the final offset inside `on_revoke`.
            let (revoked, done) = consumer.revocation().await;
            assert_eq!(revoked, [old]);
            consumer
                .consumer
                .store_offsets(HashMap::from([(old, 1)]))
                .unwrap();
            drop(done);

            // `earliest` picks up what was written before the swap.
            let queues = consumer.assignment().await;
            assert_eq!(recv_payload(&queues[0]).await, (0, "new-0".to_owned()));
            assert_eq!(consumer.consumer.generation(), 1);

            // Rolling back resumes at the offset committed on revoke, not at `old-0`.
            produce(&[(old, "old-1")]);
            consumer
                .consumer
                .push_config(&blob(&broker, old_topic.topic))
                .unwrap();
            let (revoked, _) = consumer.revocation().await;
            assert_eq!(revoked, [new]);
            let queues = consumer.assignment().await;
            assert_eq!(recv_payload(&queues[0]).await, (1, "old-1".to_owned()));
            assert_eq!(consumer.consumer.generation(), 2);

            assert_eq!(consumer.shutdown().await, [old]);
        });
    }

    #[test]
    fn test_bad_configs_keep_the_running_consumer() {
        let topic = TestTopic::create("reloading-consumer-probe");
        let partition = Partition::new(topic.topic, 0);
        let running = blob(&get_default_broker(), topic.topic);

        runtime().block_on(async {
            let mut consumer = TestConsumer::new(&running);
            let queues = consumer.assignment().await;

            assert!(matches!(
                consumer.consumer.push_config(b"not json"),
                Err(ReloadError::Blob(_))
            ));
            let unreachable = blob("127.0.0.1:1", topic.topic);
            assert_eq!(
                consumer.consumer.push_config(&unreachable).unwrap(),
                ReloadOutcome::Accepted
            );
            // A second attempt means the first one failed.
            timeout(TIMEOUT, async {
                while consumer
                    .consumer
                    .inner
                    .probe_attempts
                    .load(Ordering::SeqCst)
                    < 2
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .unwrap();

            produce(&[(partition, "0")]);
            assert_eq!(recv_payload(&queues[0]).await, (0, "0".to_owned()));
            assert_eq!(consumer.consumer.generation(), 0);
            assert_eq!(consumer.shutdown().await, [partition]);
        });
    }

    #[test]
    fn test_revert_during_revoke_restarts_the_running_config() {
        let old_topic = TestTopic::create("reloading-consumer-revert-old");
        let new_topic = TestTopic::create("reloading-consumer-revert-new");
        let old = Partition::new(old_topic.topic, 0);
        let broker = get_default_broker();
        let running = blob(&broker, old_topic.topic);

        runtime().block_on(async {
            let mut consumer = TestConsumer::new(&running);
            consumer.assignment().await;

            consumer
                .consumer
                .push_config(&blob(&broker, new_topic.topic))
                .unwrap();
            // The old consumer is stopping, so going back has to restart it, not finish the move.
            let (revoked, done) = consumer.revocation().await;
            assert_eq!(revoked, [old]);
            assert_eq!(
                consumer.consumer.push_config(&running).unwrap(),
                ReloadOutcome::Unchanged
            );
            drop(done);

            let queues = consumer.assignment().await;
            assert_eq!(queues[0].partition(), old);
            assert_eq!(consumer.consumer.generation(), 0);
            assert_eq!(consumer.shutdown().await, [old]);
        });
    }

    #[test]
    fn test_push_during_revoke_skips_the_superseded_config() {
        let old_topic = TestTopic::create("reloading-consumer-skip-old");
        let stale_topic = TestTopic::create("reloading-consumer-skip-stale");
        let new_topic = TestTopic::create("reloading-consumer-skip-new");
        let old = Partition::new(old_topic.topic, 0);
        let new = Partition::new(new_topic.topic, 0);
        let broker = get_default_broker();

        runtime().block_on(async {
            let mut consumer = TestConsumer::new(&blob(&broker, old_topic.topic));
            consumer.assignment().await;

            consumer
                .consumer
                .push_config(&blob(&broker, stale_topic.topic))
                .unwrap();
            let (_, done) = consumer.revocation().await;
            consumer
                .consumer
                .push_config(&blob(&broker, new_topic.topic))
                .unwrap();
            drop(done);

            let mut queues = consumer.assignment().await;
            // The restarted old consumer may get its partitions before the newest config
            // replaces it.
            if queues[0].partition() == old {
                let (revoked, _) = consumer.revocation().await;
                assert_eq!(revoked, [old]);
                queues = consumer.assignment().await;
            }
            assert_eq!(queues[0].partition(), new);
            // One swap: the stale config never went live.
            assert_eq!(consumer.consumer.generation(), 1);
            assert_eq!(consumer.shutdown().await, [new]);
        });
    }
}
