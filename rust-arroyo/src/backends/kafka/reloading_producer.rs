//! A producer that swaps its Kafka client when config changes.
//!
//! # Ordering across a swap
//!
//! A keyed message sent through the new client could overtake an older message
//! queued on the old client. During a swap, produce calls therefore append to a
//! buffer while the old client drains. The new client is then installed and
//! the buffer is replayed under the write lock. Produce does not wait for the
//! drain.
//!
//! Unkeyed messages also buffer. Sending them to the old client would prevent
//! its queue from emptying, while sending them to the new client would bypass
//! the buffered keyed messages.
//!
//! [`ReloadConfig::ignore_key_ordering`] skips the buffer, installs the new
//! client immediately, and drains the old client in the background.
//!
//! The buffer is capped by [`ReloadConfig::max_buffered_messages`]. A timed-out
//! drain drops messages left on the old client because rdkafka purges them when
//! the client is dropped.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex, RwLock};
use rdkafka::error::KafkaError;

use super::config::KafkaConfig;
use super::config_blob::{ConfigBlob, ConfigBlobError, ProducerSelector};
use super::producer::KafkaProducer;
use super::types::KafkaPayload;
use crate::backends::{Producer as ArroyoProducer, ProducerError};
use crate::types::TopicOrPartition;

/// Knobs for how reloads are carried out.
#[derive(Debug, Clone)]
pub struct ReloadConfig {
    /// How long to flush the old client before dropping it and purging what
    /// remains. Produce calls buffer during this time.
    pub drain_timeout: Duration,
    /// Maximum messages held while the old client drains. Produce calls fail
    /// once this is reached. Keep it below `queue.buffering.max.messages`.
    pub max_buffered_messages: usize,
    /// Upper bound on the random delay before a swap. Spreads swaps across a
    /// fleet so a config change does not stall every pod at once.
    pub jitter: Duration,
    /// How long the worker waits for the new client to reach a broker before
    /// swapping it in. `None` skips the check.
    pub probe_timeout: Option<Duration>,
    /// Delay between failed broker probes. Probes continue until one succeeds
    /// or the config is superseded.
    pub probe_retry_interval: Duration,
    /// Install the new client immediately and drain the old one afterwards,
    /// with no buffering.
    ///
    /// Set this only when same-key message order does not matter.
    pub ignore_key_ordering: bool,
}

impl Default for ReloadConfig {
    fn default() -> Self {
        Self {
            drain_timeout: Duration::from_secs(10),
            max_buffered_messages: 10_000,
            jitter: Duration::ZERO,
            probe_timeout: Some(Duration::from_secs(5)),
            probe_retry_interval: Duration::from_secs(5),
            ignore_key_ordering: false,
        }
    }
}

/// What a call to [`ReloadingKafkaProducer::push_config`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadOutcome {
    /// The valid, changed config was handed to the worker. It is not live yet;
    /// watch [`ReloadingKafkaProducer::generation`] for the swap.
    Accepted,
    /// The blob parsed, but produced the same config as the running client.
    /// Any config queued but not yet applied is dropped.
    Unchanged,
}

struct Current {
    producer: Arc<KafkaProducer>,
    config: KafkaConfig,
    /// Set while `producer` is draining for a swap. Produce calls append here
    /// instead of enqueueing on a client that is on its way out.
    ///
    /// The worker changes this only under the write lock, so a produce call
    /// sees either a usable client or the buffer.
    buffer: Option<Mutex<Vec<(TopicOrPartition, KafkaPayload)>>>,
}

/// A producer whose configuration can be replaced while it is running.
///
/// Cheap to clone; clones share one underlying client and all see a reload.
#[derive(Clone)]
pub struct ReloadingKafkaProducer {
    inner: Arc<Inner>,
    /// Its final `Drop` stops the worker, which holds only a weak reference.
    _shutdown: Arc<ShutdownOnDrop>,
}

/// Stops the reload worker once every user-facing handle is dropped.
struct ShutdownOnDrop {
    inner: Arc<Inner>,
}

impl Drop for ShutdownOnDrop {
    fn drop(&mut self) {
        self.inner.reload.lock().shutdown = true;
        self.inner.reload_wake.notify_all();
    }
}

struct Inner {
    /// Read on every produce, taken for writing only to start buffering and
    /// to install.
    current: RwLock<Current>,
    /// Serializes reloads and holds the state a swap needs.
    reload: Mutex<ReloadState>,
    /// Signalled when `reload.desired` or `reload.shutdown` changes.
    reload_wake: Condvar,
    selector: ProducerSelector,
    settings: ReloadConfig,
    generation: AtomicU64,
    /// `client.id`, for metric tags. Resolved once: the selector this producer
    /// serves is fixed, so the name is too.
    producer_name: String,
    /// Test-only queue depth when the old client was swapped out.
    #[cfg(test)]
    queue_depth_at_swap: std::sync::atomic::AtomicI32,
    /// Test-only messages purged when the retired client was dropped.
    #[cfg(test)]
    purged_at_drop: AtomicU64,
    /// Test-only calls to `drain`.
    #[cfg(test)]
    drains: AtomicU64,
    /// Test-only gate that holds the worker inside `probe`.
    #[cfg(test)]
    probe_gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    /// Test-only probe attempt count.
    #[cfg(test)]
    probe_attempts: AtomicU64,
    /// Test-only gate immediately before `install`.
    ///
    /// This must sit after the drain and before `install` takes `reload`.
    /// Moving it earlier lets `swap` catch the push and weakens the test into a
    /// duplicate of `test_revert_cancels_an_in_flight_rollout`.
    #[cfg(test)]
    swap_gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    /// Test-only completed rollout count.
    #[cfg(test)]
    rollouts_finished: AtomicU64,
}

struct ReloadState {
    /// Desired config. A push overwrites it and a revert clears it, which can
    /// cancel an in-flight rollout.
    desired: Option<KafkaConfig>,
    /// When `desired` was last set, to measure how long a config took to go
    /// live. Reset on overwrite, so it always belongs to the current `desired`.
    desired_since: Option<Instant>,
    /// Set when every handle is dropped, to stop the worker.
    shutdown: bool,
}

impl ReloadingKafkaProducer {
    /// Builds a producer from an initial config blob.
    ///
    /// Pass the opaque config bytes through. `selector` chooses the topic and
    /// application config.
    pub fn new(
        blob: &[u8],
        selector: ProducerSelector,
        settings: ReloadConfig,
    ) -> Result<Self, ReloadError> {
        let parsed = ConfigBlob::parse(blob).and_then(|blob| blob.producer_config(&selector));

        let config = match parsed {
            Ok(config) => config,
            Err(error) => {
                // Tagged `initial` because a blob that is bad at startup and one
                // that goes bad later are different problems: the first fails a
                // deploy, the second leaves a pod running stale config.
                metrics::counter!(
                    "arroyo.producer.config_reload_rejected",
                    "topic" => selector.topic().to_owned(),
                    "producer_name" => UNNAMED_PRODUCER,
                    "source" => "initial",
                    "reason" => blob_error_reason(&error),
                )
                .increment(1);
                return Err(error.into());
            }
        };

        Self::from_config(config, selector, settings)
    }

    /// Builds a producer from an already-resolved config.
    ///
    /// For applications not yet on the central configmap that still want
    /// reloading. Once a blob is pushed in, it takes over.
    pub fn from_config(
        config: KafkaConfig,
        selector: ProducerSelector,
        settings: ReloadConfig,
    ) -> Result<Self, ReloadError> {
        let producer_name = producer_name_of(&config);
        let producer = Arc::new(KafkaProducer::new(config.clone())?);
        tracing::info!(
            topic = %selector.topic(),
            app = %selector.app(),
            producer_name = %producer_name,
            "loaded initial producer config"
        );

        metrics::counter!(
            "arroyo.producer.config_reload_loaded",
            "topic" => selector.topic().to_owned(),
            "producer_name" => producer_name.clone(),
        )
        .increment(1);

        let inner = Arc::new(Inner {
            current: RwLock::new(Current {
                producer,
                config,
                buffer: None,
            }),
            reload: Mutex::new(ReloadState {
                desired: None,
                desired_since: None,
                shutdown: false,
            }),
            reload_wake: Condvar::new(),
            selector,
            settings,
            generation: AtomicU64::new(0),
            producer_name,
            #[cfg(test)]
            queue_depth_at_swap: std::sync::atomic::AtomicI32::new(0),
            #[cfg(test)]
            purged_at_drop: AtomicU64::new(0),
            #[cfg(test)]
            drains: AtomicU64::new(0),
            #[cfg(test)]
            probe_gate: Mutex::new(None),
            #[cfg(test)]
            probe_attempts: AtomicU64::new(0),
            #[cfg(test)]
            swap_gate: Mutex::new(None),
            #[cfg(test)]
            rollouts_finished: AtomicU64::new(0),
        });

        // A weak reference lets the worker exit after the last handle drops.
        let weak = Arc::downgrade(&inner);
        std::thread::Builder::new()
            .name("arroyo-producer-reload".to_owned())
            .spawn(move || {
                // If every handle was dropped before the worker started, there
                // is nothing to do.
                let Some(inner) = weak.upgrade() else { return };
                inner.run_worker();
            })
            .map_err(ReloadError::WorkerSpawn)?;

        Ok(Self {
            inner: inner.clone(),
            _shutdown: Arc::new(ShutdownOnDrop { inner }),
        })
    }

    /// Hands a new config blob to the producer.
    ///
    /// Parses and hands a changed blob to the worker. Invalid blobs leave the
    /// current client untouched. [`ReloadOutcome::Accepted`] does not mean the
    /// config is live; watch [`ReloadingKafkaProducer::generation`].
    pub fn push_config(&self, blob: &[u8]) -> Result<ReloadOutcome, ReloadError> {
        let parsed =
            ConfigBlob::parse(blob).and_then(|blob| blob.producer_config(&self.inner.selector));

        let config = match parsed {
            Ok(config) => config,
            Err(error) => {
                // Named after the running client: a blob that does not parse
                // has no name of its own, and what an operator needs to know is
                // which producer is now stuck on stale config.
                metrics::counter!(
                    "arroyo.producer.config_reload_rejected",
                    "topic" => self.inner.selector.topic().to_owned(),
                    "producer_name" => self.inner.producer_name.clone(),
                    "source" => "push",
                    "reason" => blob_error_reason(&error),
                )
                .increment(1);
                tracing::error!(
                    topic = %self.inner.selector.topic(),
                    %error,
                    "could not resolve the pushed config blob, staying on the running config"
                );
                return Err(error.into());
            }
        };

        Ok(self.inner.set_desired(config))
    }

    /// Number of swaps so far. Useful as a metric tag to tell which config a
    /// message was produced under.
    pub fn generation(&self) -> u64 {
        self.inner.generation.load(Ordering::SeqCst)
    }

    /// Messages accepted but not yet delivered, including any held while a
    /// reload drains the old client.
    pub fn in_flight_count(&self) -> i32 {
        let current = self.inner.current.read();
        let buffered = current
            .buffer
            .as_ref()
            .map_or(0, |buffer| buffer.lock().len());

        current
            .producer
            .in_flight_count()
            .saturating_add(buffered.try_into().unwrap_or(i32::MAX))
    }
}

impl Inner {
    /// Records whether a rollout is pending. Call under `reload` after each
    /// change to `desired` so the gauge cannot drift.
    fn record_pending(&self, reload: &ReloadState) {
        metrics::gauge!(
            "arroyo.producer.config_reload_pending",
            "topic" => self.selector.topic().to_owned(),
            "producer_name" => self.producer_name.clone(),
        )
        .set(u8::from(reload.desired.is_some()));
    }

    /// Records the config the host wants running and wakes the worker.
    fn set_desired(self: &Arc<Self>, config: KafkaConfig) -> ReloadOutcome {
        let mut reload = self.reload.lock();

        // Lock order is `reload` then `current`, matching `swap`.
        if config_eq(&config, &self.current.read().config) {
            // Clearing `desired` cancels any rollout back to this current config.
            reload.desired = None;
            reload.desired_since = None;
            self.reload_wake.notify_all();
            self.record_pending(&reload);
            metrics::counter!(
                "arroyo.producer.config_reload_unchanged",
                "topic" => self.selector.topic().to_owned(),
                "producer_name" => self.producer_name.clone(),
            )
            .increment(1);
            return ReloadOutcome::Unchanged;
        }

        // Overwrite so only the newest config remains.
        reload.desired = Some(config);
        reload.desired_since = Some(Instant::now());
        self.reload_wake.notify_all();
        self.record_pending(&reload);

        metrics::counter!(
            "arroyo.producer.config_reload_accepted",
            "topic" => self.selector.topic().to_owned(),
            "producer_name" => self.producer_name.clone(),
        )
        .increment(1);

        ReloadOutcome::Accepted
    }

    /// Runs broker probes and swaps until every handle is dropped.
    fn run_worker(self: &Arc<Self>) {
        loop {
            let config = {
                let mut reload = self.reload.lock();

                loop {
                    if reload.shutdown {
                        return;
                    }

                    // Clone rather than take: a newer push or revert can change
                    // `desired` and supersede this rollout.
                    match reload.desired.clone() {
                        Some(config) => break config,
                        None => self.reload_wake.wait(&mut reload),
                    }
                }
            };

            self.reload_to(&config);

            #[cfg(test)]
            self.rollouts_finished.fetch_add(1, Ordering::SeqCst);

            // Clear the handled config, but preserve a newer one.
            //
            // Known gap: the lock is released between abandoning a rollout and
            // getting here, and a config re-pushed inside that window is
            // indistinguishable from the one just handled, so it gets cleared
            // and never rolls out despite `push_config` returning `Accepted`.
            // Needs a sequence number on `desired` to fix properly.
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

    /// Probes a config and swaps to it, retrying until it is healthy or the
    /// host stops wanting it.
    fn reload_to(self: &Arc<Self>, config: &KafkaConfig) {
        // Build once; rebuilding would spawn rdkafka threads on every retry.
        let candidate = match KafkaProducer::new(config.clone()) {
            Ok(producer) => Arc::new(producer),
            Err(error) => {
                metrics::counter!(
                    "arroyo.producer.config_reload_rejected",
                    "topic" => self.selector.topic().to_owned(),
                    "producer_name" => self.producer_name.clone(),
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

        let mut attempts = 0u64;

        loop {
            attempts += 1;

            match self.probe(&candidate) {
                Ok(()) => {
                    metrics::histogram!(
                        "arroyo.producer.config_reload_probe_attempts",
                        "topic" => self.selector.topic().to_owned(),
                        "producer_name" => self.producer_name.clone(),
                    )
                    .record(attempts as f64);

                    // `swap` re-checks that the config is still wanted itself,
                    // atomically with installing the client.
                    self.swap(candidate.clone(), config);
                    return;
                }
                Err(error) => {
                    metrics::counter!(
                        "arroyo.producer.config_reload_probe_failed",
                        "topic" => self.selector.topic().to_owned(),
                        "producer_name" => self.producer_name.clone(),
                    )
                    .increment(1);
                    tracing::warn!(
                        topic = %self.selector.topic(),
                        %error,
                        attempts,
                        "new config cannot reach a broker, staying on the running one"
                    );
                }
            }

            let mut reload = self.reload.lock();
            let interval = self.settings.probe_retry_interval;

            // Check before waiting too: a push that arrived while we were
            // blocked in the probe has already been notified, so waiting the
            // full interval first would just add latency.
            if superseded(&reload, config) {
                return;
            }

            self.reload_wake.wait_for(&mut reload, interval);

            if superseded(&reload, config) {
                return;
            }
        }
    }

    /// Checks that a newly built client can actually reach a broker.
    ///
    /// Client construction does no I/O; this prevents swapping in an
    /// unreachable producer.
    fn probe(&self, candidate: &KafkaProducer) -> Result<(), KafkaError> {
        let Some(timeout) = self.settings.probe_timeout else {
            return Ok(());
        };

        #[cfg(test)]
        self.probe_attempts.fetch_add(1, Ordering::Relaxed);

        // Test-only: wait until the test releases the gate, so the rollout is
        // observably in flight while the test pushes a new config.
        #[cfg(test)]
        {
            let gate = self.probe_gate.lock().take();
            if let Some(gate) = gate {
                let _ = gate.recv();
            }
        }

        let started = Instant::now();
        // Topic metadata probes the broker without producing a message.
        let result =
            candidate.validate_topic(crate::types::Topic::new(self.selector.topic()), timeout);

        metrics::histogram!(
            "arroyo.producer.config_reload_probe_ms",
            "topic" => self.selector.topic().to_owned(),
            "producer_name" => self.producer_name.clone(),
        )
        .record(started.elapsed().as_millis() as f64);

        result
    }

    /// Drains the old client and installs an already-probed one in its place.
    fn swap(self: &Arc<Self>, new_producer: Arc<KafkaProducer>, config: &KafkaConfig) {
        // Spread swaps across instances.
        if !self.settings.jitter.is_zero() {
            let jitter = rand::random::<f64>() * self.settings.jitter.as_secs_f64();
            std::thread::sleep(Duration::from_secs_f64(jitter));
        }

        // Bail out before doing any work if the host already wants something
        // else. `install` checks again, atomically with the install itself;
        // this one just avoids buffering and draining for nothing.
        if superseded(&self.reload.lock(), config) {
            self.record_superseded(0);
            return;
        }

        // Start buffering, so the old client stops receiving messages and its
        // queue can actually reach zero. Produce calls append to the buffer
        // from here until `install` takes it away again.
        //
        // Skipped when ordering does not matter: the new client then goes in
        // straight away and the old one drains behind it.
        if !self.settings.ignore_key_ordering {
            let mut current = self.current.write();
            current.buffer = Some(Mutex::new(Vec::new()));
            let draining = current.producer.clone();
            drop(current);

            // Nothing feeds this client now, so the flush can converge.
            self.drain(&draining, self.settings.drain_timeout);
        }

        // Test-only: park here, with the buffer live and the old client
        // drained, so a test can land a push in the window that the atomic
        // re-check inside `install` guards.
        #[cfg(test)]
        {
            let gate = self.swap_gate.lock().take();
            if let Some(gate) = gate {
                let _ = gate.recv();
            }
        }

        let Some(old) = self.install(new_producer, config) else {
            return;
        };

        // Only the unordered path still has a client to drain. On the ordered
        // path it was drained before the install, and anything the drain did
        // not get to has to be purged rather than delivered: the buffer has
        // just gone out on the new client, so delivering older same-key
        // messages now is the reorder this whole design exists to prevent.
        if self.settings.ignore_key_ordering {
            self.drain(&old, self.settings.drain_timeout);
        }

        let purged = old.in_flight_count().max(0) as u64;

        #[cfg(test)]
        self.purged_at_drop.store(purged, Ordering::SeqCst);

        if purged > 0 {
            metrics::counter!(
                "arroyo.producer.config_reload_purged_messages",
                "topic" => self.selector.topic().to_owned(),
                "producer_name" => self.producer_name.clone(),
            )
            .increment(purged);
            tracing::error!(
                topic = %self.selector.topic(),
                purged,
                "dropping the old client with messages still queued, they are lost"
            );
        }

        drop(old);
    }

    /// Installs a drained client and returns the one it replaced, or `None` if
    /// the config went stale, in which case the buffer goes back to the client
    /// that stays.
    fn install(
        self: &Arc<Self>,
        new_producer: Arc<KafkaProducer>,
        config: &KafkaConfig,
    ) -> Option<Arc<KafkaProducer>> {
        // Hold `reload` across the superseded check and the install; otherwise
        // a push between them could install an unwanted client. Lock order is
        // `reload` then `current`, matching `set_desired`; never reverse it.
        let reload = self.reload.lock();
        let superseded = superseded(&reload, config);
        let waited_for = reload.desired_since.map(|since| since.elapsed());

        let (old, buffered) = {
            let mut current = self.current.write();

            // Taking and replaying the buffer under the same guard as the
            // install is what keeps the order intact: nothing can enqueue in
            // between, so the buffer goes out ahead of anything produced after.
            let buffer = current.buffer.take();

            let old = (!superseded).then(|| {
                #[cfg(test)]
                self.queue_depth_at_swap
                    .store(current.producer.in_flight_count(), Ordering::SeqCst);

                current.config = config.clone();
                std::mem::replace(&mut current.producer, new_producer)
            });

            let buffered = match buffer {
                Some(buffer) => self.replay(buffer.into_inner(), &current.producer),
                None => 0,
            };

            (old, buffered)
        };

        drop(reload);

        let Some(old) = old else {
            self.record_superseded(buffered);
            return None;
        };

        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        metrics::counter!(
            "arroyo.producer.config_reload_applied",
            "topic" => self.selector.topic().to_owned(),
            "producer_name" => self.producer_name.clone(),
        )
        .increment(1);

        metrics::gauge!(
            "arroyo.producer.config_reload_generation",
            "topic" => self.selector.topic().to_owned(),
            "producer_name" => self.producer_name.clone(),
        )
        .set(generation as f64);

        if let Some(waited_for) = waited_for {
            metrics::histogram!(
                "arroyo.producer.config_reload_latency_ms",
                "topic" => self.selector.topic().to_owned(),
                "producer_name" => self.producer_name.clone(),
            )
            .record(waited_for.as_millis() as f64);
        }

        tracing::info!(
            topic = %self.selector.topic(),
            producer_name = %self.producer_name,
            generation,
            latency_ms = waited_for.map(|waited| waited.as_millis() as u64),
            buffered,
            "config reload applied"
        );

        Some(old)
    }

    fn record_superseded(&self, buffered: usize) {
        metrics::counter!(
            "arroyo.producer.config_reload_superseded",
            "topic" => self.selector.topic().to_owned(),
            "producer_name" => self.producer_name.clone(),
        )
        .increment(1);
        tracing::info!(
            topic = %self.selector.topic(),
            buffered,
            "config superseded before it went live, not swapping to it"
        );
    }

    /// Hands buffered messages to `producer`, in the order they were produced.
    /// Returns how many there were.
    fn replay(
        &self,
        buffer: Vec<(TopicOrPartition, KafkaPayload)>,
        producer: &KafkaProducer,
    ) -> usize {
        let buffered = buffer.len();

        metrics::histogram!(
            "arroyo.producer.config_reload_buffered_messages",
            "topic" => self.selector.topic().to_owned(),
            "producer_name" => self.producer_name.clone(),
        )
        .record(buffered as f64);

        for (destination, payload) in buffer {
            // The produce call that buffered this already returned Ok, so
            // there is nobody left to report the error to.
            if let Err(error) = producer.produce(&destination, payload) {
                metrics::counter!(
                    "arroyo.producer.config_reload_replay_failed",
                    "topic" => self.selector.topic().to_owned(),
                    "producer_name" => self.producer_name.clone(),
                )
                .increment(1);
                tracing::error!(
                    topic = %self.selector.topic(),
                    %error,
                    "could not replay a buffered message onto the new client"
                );
            }
        }

        buffered
    }

    /// Flushes a client that no longer accepts messages.
    fn drain(&self, producer: &KafkaProducer, timeout: Duration) {
        #[cfg(test)]
        self.drains.fetch_add(1, Ordering::SeqCst);

        let started = Instant::now();
        let result = producer.flush_for(timeout);
        let elapsed = started.elapsed();

        metrics::histogram!(
            "arroyo.producer.config_reload_drain_ms",
            "topic" => self.selector.topic().to_owned(),
            "producer_name" => self.producer_name.clone(),
        )
        .record(elapsed.as_millis() as f64);

        match result {
            Ok(()) => tracing::info!(
                topic = %self.selector.topic(),
                producer_name = %self.producer_name,
                drain_ms = elapsed.as_millis() as u64,
                "drained old producer for config reload"
            ),
            Err(error) => {
                metrics::counter!(
                    "arroyo.producer.config_reload_drain_timeout",
                    "topic" => self.selector.topic().to_owned(),
                    "producer_name" => self.producer_name.clone(),
                )
                .increment(1);
                tracing::error!(
                    topic = %self.selector.topic(),
                    producer_name = %self.producer_name,
                    %error,
                    queued = producer.in_flight_count(),
                    "old producer did not drain in time, its queued messages will be dropped"
                );
            }
        }
    }
}

impl ArroyoProducer<KafkaPayload> for ReloadingKafkaProducer {
    fn produce(
        &self,
        destination: &TopicOrPartition,
        payload: KafkaPayload,
    ) -> Result<(), ProducerError> {
        // The read guard is the whole of the coordination with a reload: the
        // swap takes the write lock to start buffering and again to install and
        // replay, so this either enqueues on a client that is still live or
        // lands in the buffer, never in between.
        let current = self.inner.current.read();

        let Some(buffer) = &current.buffer else {
            return current.producer.produce(destination, payload);
        };

        let mut buffer = buffer.lock();

        if buffer.len() >= self.inner.settings.max_buffered_messages {
            metrics::counter!(
                "arroyo.producer.config_reload_buffer_full",
                "topic" => self.inner.selector.topic().to_owned(),
                "producer_name" => self.inner.producer_name.clone(),
            )
            .increment(1);
            return Err(ProducerError::ProducerFailure {
                error: "reload buffer is full".to_owned(),
            });
        }

        buffer.push((*destination, payload));

        Ok(())
    }
}

/// Something went wrong while loading or applying a config.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ReloadError {
    #[error("invalid config blob")]
    Blob(#[from] ConfigBlobError),

    #[error("kafka rejected the new configuration")]
    Kafka(#[from] KafkaError),

    #[error("could not spawn the config reload worker")]
    WorkerSpawn(#[source] std::io::Error),
}

/// Default `client.id` before config is resolved.
const UNNAMED_PRODUCER: &str = "unknown";

/// Returns the producer name used in metric tags.
fn producer_name_of(config: &KafkaConfig) -> String {
    config
        .get_config_value("client.id")
        .cloned()
        .unwrap_or_else(|| UNNAMED_PRODUCER.to_owned())
}

/// Metric tag for why a pushed blob was rejected. Low cardinality on purpose:
/// the offending topic or variable goes in the log line, not the tag.
fn blob_error_reason(error: &ConfigBlobError) -> &'static str {
    match error {
        ConfigBlobError::Malformed(_) => "malformed",
        ConfigBlobError::UnknownTopic { .. } => "unknown_topic",
        ConfigBlobError::UnknownCluster { .. } => "unknown_cluster",
        ConfigBlobError::MissingEnvVar { .. } => "missing_env_var",
        ConfigBlobError::UnexpectedLogicalTopic { .. } => "unexpected_logical_topic",
    }
}

fn superseded(reload: &ReloadState, config: &KafkaConfig) -> bool {
    reload.shutdown
        || !reload
            .desired
            .as_ref()
            .is_some_and(|desired| config_eq(desired, config))
}

/// Compares only the parameter map. `offset_reset_config` is consumer-only and
/// never set by the producer paths, so it cannot differ here.
fn config_eq(left: &KafkaConfig, right: &KafkaConfig) -> bool {
    left.config_map() == right.config_map()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::mocking::MockCluster;
    use std::sync::mpsc;

    fn blob(servers: &str, acks: &str) -> Vec<u8> {
        format!(
            r#"{{
                "clusters": {{"events": {{"config": {{"bootstrap.servers": "{servers}"}}}}}},
                "topics": {{
                    "ingest-events": {{
                        "cluster": "events",
                        "producer_config": {{"acks": "{acks}"}}
                    }}
                }}
            }}"#
        )
        .into_bytes()
    }

    /// Builds a config whose queue does not drain by itself during tests.
    fn lingering_blob(servers: &str, acks: &str) -> Vec<u8> {
        format!(
            r#"{{
                "clusters": {{"events": {{"config": {{"bootstrap.servers": "{servers}"}}}}}},
                "topics": {{
                    "ingest-events": {{
                        "cluster": "events",
                        "producer_config": {{"acks": "{acks}", "linger.ms": "1000"}}
                    }}
                }}
            }}"#
        )
        .into_bytes()
    }

    fn settings() -> ReloadConfig {
        ReloadConfig {
            drain_timeout: Duration::from_secs(5),
            max_buffered_messages: 10_000,
            jitter: Duration::ZERO,
            // MockCluster answers metadata, so probing stays on in tests.
            probe_timeout: Some(Duration::from_secs(5)),
            probe_retry_interval: Duration::from_millis(50),
            ignore_key_ordering: false,
        }
    }

    #[track_caller]
    fn await_generation(producer: &ReloadingKafkaProducer, generation: u64) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while producer.generation() < generation {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for generation {generation}, at {}",
                producer.generation()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Waits for post-install drain and drop work to finish.
    #[track_caller]
    fn await_rollout(producer: &ReloadingKafkaProducer, rollouts: u64) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while producer.inner.rollouts_finished.load(Ordering::SeqCst) < rollouts {
            assert!(Instant::now() < deadline, "timed out waiting for a rollout");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[track_caller]
    fn wait_until_probing(producer: &ReloadingKafkaProducer) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while producer.inner.probe_gate.lock().is_some() {
            assert!(Instant::now() < deadline, "worker never reached the probe");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[track_caller]
    fn assert_stays_at_generation(producer: &ReloadingKafkaProducer, generation: u64) {
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(producer.generation(), generation);
    }

    fn selector() -> ProducerSelector {
        ProducerSelector::new("ingest-events", "relay")
    }

    fn producer(servers: &str) -> ReloadingKafkaProducer {
        ReloadingKafkaProducer::new(&blob(servers, "all"), selector(), settings()).unwrap()
    }

    #[test]
    fn test_reload_replaces_the_client() {
        let cluster = MockCluster::new(1).unwrap();
        let producer = producer(&cluster.bootstrap_servers());
        assert_eq!(producer.generation(), 0);

        let outcome = producer
            .push_config(&blob(&cluster.bootstrap_servers(), "1"))
            .unwrap();

        // The swap is handed to the worker, not done inline.
        assert_eq!(outcome, ReloadOutcome::Accepted);
        await_generation(&producer, 1);
    }

    #[test]
    fn test_identical_config_does_not_swap() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);

        let outcome = producer.push_config(&blob(&servers, "all")).unwrap();

        assert_eq!(outcome, ReloadOutcome::Unchanged);
        assert_stays_at_generation(&producer, 0);
    }

    #[test]
    fn test_invalid_blob_keeps_the_running_client() {
        let cluster = MockCluster::new(1).unwrap();
        let producer = producer(&cluster.bootstrap_servers());

        assert!(producer.push_config(b"{not json").is_err());
        assert!(producer.push_config(br#"{"topics": {}}"#).is_err());

        assert_stays_at_generation(&producer, 0);
        producer
            .produce(
                &TopicOrPartition::Topic(crate::types::Topic::new("ingest-events")),
                KafkaPayload::new(None, None, Some(b"payload".to_vec())),
            )
            .unwrap();
    }

    #[test]
    fn test_config_rejected_by_rdkafka_keeps_the_running_client() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);

        let bad = String::from_utf8(blob(&servers, "all")).unwrap().replace(
            r#""acks": "all""#,
            r#""message.timeout.ms": "not-a-number""#,
        );

        // The blob is well-formed, so it is accepted; rdkafka rejects it when
        // the worker builds the client, and no swap happens.
        assert_eq!(
            producer.push_config(bad.as_bytes()).unwrap(),
            ReloadOutcome::Accepted
        );
        assert_stays_at_generation(&producer, 0);

        producer
            .produce(
                &TopicOrPartition::Topic(crate::types::Topic::new("ingest-events")),
                KafkaPayload::new(None, None, Some(b"payload".to_vec())),
            )
            .unwrap();
    }

    /// A failing probe retries until a reachable config supersedes it.
    #[test]
    fn test_failed_probe_is_superseded_by_a_good_config() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = ReloadingKafkaProducer::new(
            &blob(&servers, "all"),
            selector(),
            ReloadConfig {
                probe_timeout: Some(Duration::from_millis(300)),
                probe_retry_interval: Duration::from_millis(50),
                ..settings()
            },
        )
        .unwrap();

        producer.push_config(&blob("127.0.0.1:1", "all")).unwrap();
        assert_stays_at_generation(&producer, 0);

        producer.push_config(&blob(&servers, "1")).unwrap();
        await_generation(&producer, 1);
        assert_eq!(
            producer
                .inner
                .current
                .read()
                .config
                .get_config_value("acks"),
            Some(&"1".to_string())
        );
    }

    /// A burst of pushes must cost one swap, not one per push.
    ///
    /// There is no debounce timer: `desired` is a single slot, so pushes that
    /// arrive while the worker is busy overwrite each other and only the
    /// newest is ever taken. The probe gate holds the worker busy for the
    /// duration of the burst, making this deterministic.
    #[test]
    fn test_burst_collapses_to_one_swap() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);

        // Hold the worker inside the probe so the whole burst lands while it
        // is busy.
        let (release, gate) = mpsc::channel();
        *producer.inner.probe_gate.lock() = Some(gate);

        producer.push_config(&blob(&servers, "1")).unwrap();
        wait_until_probing(&producer);

        for acks in ["0", "2", "3"] {
            assert_eq!(
                producer.push_config(&blob(&servers, acks)).unwrap(),
                ReloadOutcome::Accepted
            );
        }

        release.send(()).unwrap();

        // The first config was superseded and the burst collapsed, so exactly
        // one swap happens and it carries the newest config.
        await_generation(&producer, 1);
        assert_stays_at_generation(&producer, 1);
        assert_eq!(
            producer
                .inner
                .current
                .read()
                .config
                .get_config_value("acks"),
            Some(&"3".to_string())
        );
    }

    /// A pending config replaced before the worker reaches it is dropped, and
    /// re-pushing the config already rolling out does not disturb it.
    #[test]
    fn test_pending_config_replaced_before_pickup_is_dropped() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);

        let (release, gate) = mpsc::channel();
        *producer.inner.probe_gate.lock() = Some(gate);

        producer.push_config(&blob(&servers, "1")).unwrap();
        wait_until_probing(&producer);

        producer.push_config(&blob(&servers, "0")).unwrap();
        producer.push_config(&blob(&servers, "1")).unwrap();

        release.send(()).unwrap();
        await_generation(&producer, 1);

        assert_stays_at_generation(&producer, 1);
        assert_eq!(
            producer
                .inner
                .current
                .read()
                .config
                .get_config_value("acks"),
            Some(&"1".to_string())
        );
    }

    #[test]
    fn test_produce_works_across_a_reload() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);
        let destination = TopicOrPartition::Topic(crate::types::Topic::new("ingest-events"));

        for i in 0..5 {
            let keyed = i % 2 == 0;
            producer
                .produce(
                    &destination,
                    KafkaPayload::new(
                        keyed.then(|| b"key".to_vec()),
                        None,
                        Some(format!("msg-{i}").into_bytes()),
                    ),
                )
                .unwrap();

            if i == 2 {
                producer.push_config(&blob(&servers, "1")).unwrap();
            }
        }

        await_generation(&producer, 1);
    }

    /// The old queue must be empty at the swap or a same-key message on the
    /// new client can overtake it. The lingering config makes draining the only
    /// way to empty the queue.
    #[test]
    fn test_old_queue_is_empty_when_the_swap_happens() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer =
            ReloadingKafkaProducer::new(&lingering_blob(&servers, "all"), selector(), settings())
                .unwrap();
        let destination = TopicOrPartition::Topic(crate::types::Topic::new("ingest-events"));

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let producer = producer.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    producer
                        .produce(
                            &destination,
                            KafkaPayload::new(Some(b"key".to_vec()), None, Some(b"keyed".to_vec())),
                        )
                        .unwrap();
                    std::thread::sleep(Duration::from_millis(1));
                }
            })
        };

        producer
            .push_config(&lingering_blob(&servers, "1"))
            .unwrap();
        await_generation(&producer, 1);

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        writer.join().unwrap();

        assert_eq!(
            producer.inner.queue_depth_at_swap.load(Ordering::SeqCst),
            0,
            "swapped out a client that still had messages queued"
        );
    }

    /// Produce must not stall during a drain. An unreachable broker forces
    /// this drain to run for the full timeout.
    #[test]
    fn test_produce_does_not_wait_for_a_drain_that_never_finishes() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();

        let producer = ReloadingKafkaProducer::new(
            &blob("127.0.0.1:1", "all"),
            selector(),
            ReloadConfig {
                drain_timeout: Duration::from_secs(30),
                ..settings()
            },
        )
        .unwrap();
        let destination = TopicOrPartition::Topic(crate::types::Topic::new("ingest-events"));

        producer
            .produce(
                &destination,
                KafkaPayload::new(Some(b"key".to_vec()), None, Some(b"stuck".to_vec())),
            )
            .unwrap();

        producer.push_config(&blob(&servers, "1")).unwrap();

        // Wait for buffering to come on, so the count below is not polluted by
        // produce calls that legitimately went straight to the old client while
        // the worker was still probing.
        let deadline = Instant::now() + Duration::from_secs(5);
        while producer.inner.current.read().buffer.is_none() {
            assert!(
                Instant::now() < deadline,
                "worker never turned buffering on"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        let deadline = Instant::now() + Duration::from_secs(3);
        let mut worst = Duration::ZERO;
        let mut produced = 0;

        while Instant::now() < deadline {
            let started = Instant::now();
            producer
                .produce(
                    &destination,
                    KafkaPayload::new(Some(b"key".to_vec()), None, Some(b"keyed".to_vec())),
                )
                .unwrap();
            worst = worst.max(started.elapsed());
            produced += 1;
            std::thread::sleep(Duration::from_millis(1));
        }

        assert!(
            worst < Duration::from_millis(500),
            "produce blocked for {worst:?} while a drain was in progress"
        );

        // Not blocking is only half of it: those messages must have gone into
        // the buffer, not onto the client that is being retired. Without this
        // the test passes against an implementation that never buffers at all.
        let current = producer.inner.current.read();
        assert_eq!(
            current
                .buffer
                .as_ref()
                .expect("buffering went away")
                .lock()
                .len(),
            produced
        );
        assert_eq!(
            current.producer.in_flight_count(),
            1,
            "produce reached the draining client instead of the buffer"
        );

        assert_eq!(producer.generation(), 0);
    }

    /// Dropping every handle must stop the reload worker, otherwise each
    /// producer leaks a thread for the life of the process.
    #[test]
    fn test_worker_exits_when_handles_are_dropped() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();

        let weak = {
            let producer = producer(&servers);
            let clone = producer.clone();
            producer.push_config(&blob(&servers, "1")).unwrap();
            await_generation(&producer, 1);
            drop(clone);
            Arc::downgrade(&producer.inner)
        };

        // The worker holds only a weak reference, so once it observes the
        // shutdown flag and exits, `Inner` is freed.
        let deadline = Instant::now() + Duration::from_secs(10);
        while weak.upgrade().is_some() {
            assert!(
                Instant::now() < deadline,
                "reload worker still holds the producer alive"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Reverting to the running config must cancel an in-flight rollout. The
    /// probe gate makes the timing deterministic.
    #[test]
    fn test_revert_cancels_an_in_flight_rollout() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);

        // Arm the gate before pushing, so the worker cannot get past the probe
        // before the revert lands.
        let (release, gate) = mpsc::channel();
        *producer.inner.probe_gate.lock() = Some(gate);

        producer.push_config(&blob(&servers, "9")).unwrap();

        wait_until_probing(&producer);

        assert_eq!(
            producer.push_config(&blob(&servers, "all")).unwrap(),
            ReloadOutcome::Unchanged
        );

        release.send(()).unwrap();

        assert_stays_at_generation(&producer, 0);
        assert_eq!(
            producer
                .inner
                .current
                .read()
                .config
                .get_config_value("acks"),
            Some(&"all".to_string())
        );
    }

    /// A revert that lands after the probe passed but before the install must
    /// cancel the swap, and the messages buffered in the meantime must go to
    /// the client that stays rather than be dropped with the abandoned one.
    ///
    /// The superseded check and the install must be atomic, or a revert in
    /// between is lost. The worker is parked at the gate after the drain, which
    /// is the window that check guards, so the revert cannot lose a wall-clock
    /// race.
    #[test]
    fn test_revert_between_probe_and_install_cancels_the_swap() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);
        let destination = TopicOrPartition::Topic(crate::types::Topic::new("ingest-events"));

        // Arm before pushing, so the worker cannot get past the gate first.
        let (release, gate) = mpsc::channel();
        *producer.inner.swap_gate.lock() = Some(gate);

        producer.push_config(&blob(&servers, "9")).unwrap();

        // Worker parked: probe passed and the old client is drained, but the
        // install has not run.
        let deadline = Instant::now() + Duration::from_secs(5);
        while producer.inner.swap_gate.lock().is_some() {
            assert!(Instant::now() < deadline, "worker never reached the swap");
            std::thread::sleep(Duration::from_millis(5));
        }

        // Buffering is on, so these land in the buffer rather than on a client.
        for i in 0..4 {
            producer
                .produce(
                    &destination,
                    KafkaPayload::new(
                        Some(b"key".to_vec()),
                        None,
                        Some(format!("msg-{i}").into_bytes()),
                    ),
                )
                .unwrap();
        }
        assert_eq!(
            producer
                .inner
                .current
                .read()
                .buffer
                .as_ref()
                .expect("worker did not turn buffering on")
                .lock()
                .len(),
            4
        );

        // The revert lands inside the window. `Unchanged` is guaranteed here,
        // not raced for: the worker is parked before the install.
        assert_eq!(
            producer.push_config(&blob(&servers, "all")).unwrap(),
            ReloadOutcome::Unchanged
        );

        release.send(()).unwrap();

        // Wait until the rollout ended one way or the other, then check which.
        // Both a correct and a broken implementation reach this point, so the
        // assertions below are what tell them apart.
        let deadline = Instant::now() + Duration::from_secs(5);
        while producer.inner.rollouts_finished.load(Ordering::SeqCst) == 0 {
            assert!(
                Instant::now() < deadline,
                "worker never finished the rollout"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(
            producer.generation(),
            0,
            "a config reverted before install was swapped in anyway"
        );

        let current = producer.inner.current.read();
        assert_eq!(current.config.get_config_value("acks"), Some(&"all".into()));
        assert!(
            current.buffer.is_none(),
            "buffering was left on after the rollout was abandoned"
        );
    }

    /// After a revert, the worker must stop retrying the old config or it
    /// could go live if its broker later recovers.
    #[test]
    fn test_revert_stops_the_probe_retry_loop() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = ReloadingKafkaProducer::new(
            &blob(&servers, "all"),
            selector(),
            ReloadConfig {
                probe_timeout: Some(Duration::from_millis(50)),
                probe_retry_interval: Duration::from_millis(20),
                ..settings()
            },
        )
        .unwrap();

        producer.push_config(&blob("127.0.0.1:1", "9")).unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        while producer.inner.probe_attempts.load(Ordering::Relaxed) < 2 {
            assert!(Instant::now() < deadline, "worker never retried the probe");
            std::thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(
            producer.push_config(&blob(&servers, "all")).unwrap(),
            ReloadOutcome::Unchanged
        );
        let after_revert = producer.inner.probe_attempts.load(Ordering::Relaxed);

        // At most one more probe may be in flight when the revert lands; after
        // that the worker must be idle rather than still retrying.
        std::thread::sleep(Duration::from_millis(400));
        let settled = producer.inner.probe_attempts.load(Ordering::Relaxed);
        assert!(
            settled <= after_revert + 1,
            "worker kept retrying a reverted config: {after_revert} -> {settled} probes"
        );
        assert_eq!(producer.generation(), 0);
    }

    /// Pushing the same config twice while the first is rolling out must not
    /// cost a second drain and swap.
    #[test]
    fn test_duplicate_push_does_not_swap_twice() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);

        // Hold the worker in the probe for the first rollout, then push the
        // same config again. The duplicate is queued behind a rollout that is
        // about to make it a no-op, which is the case the dedup check in
        // `run_worker` exists for: without it the worker takes the duplicate
        // after the swap and pays for a second build, probe, swap and drain.
        let (release, gate) = mpsc::channel();
        *producer.inner.probe_gate.lock() = Some(gate);

        producer.push_config(&blob(&servers, "1")).unwrap();
        wait_until_probing(&producer);
        producer.push_config(&blob(&servers, "1")).unwrap();

        release.send(()).unwrap();
        await_generation(&producer, 1);

        // The duplicate resolved to the config now running, so it was dropped
        // rather than triggering another swap.
        assert_stays_at_generation(&producer, 1);
    }

    #[test]
    fn test_clones_share_one_client() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);
        let clone = producer.clone();

        clone.push_config(&blob(&servers, "1")).unwrap();

        // The reload is visible through the original handle.
        await_generation(&producer, 1);
    }

    fn start_buffering(producer: &ReloadingKafkaProducer) {
        producer.inner.current.write().buffer = Some(Mutex::new(Vec::new()));
    }

    /// Produce during a drain must return at once and use the buffer.
    #[test]
    fn test_produce_buffers_while_the_old_client_drains() {
        let cluster = MockCluster::new(1).unwrap();
        let producer = producer(&cluster.bootstrap_servers());
        let destination = TopicOrPartition::Topic(crate::types::Topic::new("ingest-events"));

        start_buffering(&producer);

        let started = Instant::now();
        for i in 0..3 {
            producer
                .produce(
                    &destination,
                    KafkaPayload::new(
                        Some(b"key".to_vec()),
                        None,
                        Some(format!("msg-{i}").into_bytes()),
                    ),
                )
                .unwrap();
        }
        assert!(started.elapsed() < Duration::from_millis(100));

        let current = producer.inner.current.read();
        let buffer = current.buffer.as_ref().expect("buffer went away");
        assert_eq!(buffer.lock().len(), 3);
        assert_eq!(
            current.producer.in_flight_count(),
            0,
            "messages reached the draining client instead of the buffer"
        );
    }

    /// A full buffer must return an error instead of growing without bound.
    #[test]
    fn test_buffer_full_is_reported_to_the_caller() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = ReloadingKafkaProducer::new(
            &blob(&servers, "all"),
            selector(),
            ReloadConfig {
                max_buffered_messages: 2,
                ..settings()
            },
        )
        .unwrap();
        let destination = TopicOrPartition::Topic(crate::types::Topic::new("ingest-events"));

        start_buffering(&producer);

        let mut produce = || {
            producer.produce(
                &destination,
                KafkaPayload::new(Some(b"key".to_vec()), None, Some(b"keyed".to_vec())),
            )
        };

        assert!(produce().is_ok());
        assert!(produce().is_ok());
        assert!(produce().is_err());
    }
    /// A swap must drain the client it retires exactly once, before the
    /// install.
    ///
    /// Draining it again afterwards would give whatever the first drain could
    /// not deliver a second chance, by which point the buffer has already gone
    /// out on the new client. Older same-key messages would then land behind
    /// newer ones, which is the reorder the buffer exists to prevent; they have
    /// to be purged instead.
    #[test]
    fn test_retired_client_is_drained_exactly_once() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);

        producer.push_config(&blob(&servers, "1")).unwrap();
        await_rollout(&producer, 1);

        assert_eq!(producer.inner.drains.load(Ordering::SeqCst), 1);
    }

    /// `ignore_key_ordering` skips the buffer but must still drain the retired
    /// client. The lingering config keeps a message queued until that drain.
    #[test]
    fn test_ignoring_ordering_still_drains_the_retired_client() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = ReloadingKafkaProducer::new(
            &lingering_blob(&servers, "all"),
            selector(),
            ReloadConfig {
                ignore_key_ordering: true,
                ..settings()
            },
        )
        .unwrap();
        let destination = TopicOrPartition::Topic(crate::types::Topic::new("ingest-events"));

        producer
            .produce(
                &destination,
                KafkaPayload::new(Some(b"key".to_vec()), None, Some(b"keyed".to_vec())),
            )
            .unwrap();

        producer
            .push_config(&lingering_blob(&servers, "1"))
            .unwrap();

        // The drain and the drop happen after the install, so waiting for the
        // generation alone would read the counter before it is written.
        await_rollout(&producer, 1);

        assert_eq!(
            producer.inner.purged_at_drop.load(Ordering::SeqCst),
            0,
            "retired the old client without draining it, its queued messages were purged"
        );

        producer
            .produce(
                &destination,
                KafkaPayload::new(Some(b"key".to_vec()), None, Some(b"keyed".to_vec())),
            )
            .unwrap();
    }
}
