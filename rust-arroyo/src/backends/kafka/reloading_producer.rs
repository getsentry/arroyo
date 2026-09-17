//! A producer that swaps its underlying Kafka client when config changes.
//!
//! The host pushes an opaque config blob at startup and on changes. Arroyo
//! validates it, detects changes, and drains and replaces the client.
//!
//! # Ordering across a swap
//!
//! Messages already queued on the old client have not been written yet. If a
//! keyed message went to the new client while an older message with the same
//! key sat in the old client's queue, the two could reach the partition out of
//! order. So keyed produce calls wait for the old client to drain, while
//! unkeyed produce calls, which have no ordering requirement, go straight to
//! the new client.
//!
//! Applications that key only to spread load, and do not care about the order
//! of same-key messages, can set [`ReloadConfig::ignore_key_ordering`] to skip
//! the wait entirely.
//!
//! If the drain outlasts [`ReloadConfig::drain_timeout`], keyed produce calls
//! stop waiting rather than blocking the application indefinitely, accepting
//! that ordering may break.
//!
//! # Messages lost on a slow drain
//!
//! rdkafka purges queued and in-flight messages when a producer is dropped, so
//! a drain timeout loses messages rather than delivering them late. Size
//! [`ReloadConfig::drain_timeout`] for the queue depth at reload time, and alert
//! on `arroyo.producer.config_reload_purged_messages`.
//!
//! # Scope
//!
//! This wraps [`KafkaProducer`] only. There is deliberately no reloading
//! counterpart for [`AsyncKafkaProducer`]: the ordering guarantee above is
//! built on a blocking condvar wait made while holding a lock, which an async
//! produce path cannot reuse. An async version would be a sibling type sharing
//! the config and settings here, not a generalization of this one.
//!
//! [`AsyncKafkaProducer`]: super::producer::AsyncKafkaProducer

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
    /// How long to drain the old client and how long keyed produces wait for it.
    pub drain_timeout: Duration,
    /// Upper bound on the random delay before a swap. Spreads swaps across a
    /// fleet so a config change does not stall every pod at once.
    pub jitter: Duration,
    /// How long to wait for the new client to reach a broker before swapping
    /// it in. `None` skips the check.
    ///
    /// Building a client does no I/O, so a config naming an unreachable broker
    /// constructs fine. Without this check such a config would retire a
    /// working producer and install a dead one. The check runs off the
    /// caller's thread, so it delays a swap rather than blocking anyone.
    pub probe_timeout: Option<Duration>,
    /// How long to wait before probing again after a failed probe.
    ///
    /// Probing retries until it succeeds or a newer config supersedes it,
    /// since an unreachable broker is as likely to be a blip as a bad config.
    pub probe_retry_interval: Duration,
    /// Treat keyed messages like unkeyed ones: never wait for a drain.
    ///
    /// Set this when the key is only there to spread load across partitions
    /// and nothing downstream depends on same-key messages arriving in order.
    /// Produce calls then never block on a reload, at the cost of the ordering
    /// guarantee described in the module docs.
    pub ignore_key_ordering: bool,
}

impl Default for ReloadConfig {
    fn default() -> Self {
        Self {
            drain_timeout: Duration::from_secs(10),
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
    /// The config is valid and differs from what is running, so it has been
    /// handed to the reload worker.
    ///
    /// The swap happens in the background once the config clears the broker
    /// probe and jitter, so the new client is not live yet when this
    /// returns. Watch [`ReloadingKafkaProducer::generation`] or the
    /// `arroyo.producer.config_reload_applied` metric for that.
    Accepted,
    /// The blob parsed, but produced the same config as the running client.
    /// Any config queued but not yet applied is dropped.
    Unchanged,
}

struct Current {
    producer: Arc<KafkaProducer>,
    config: KafkaConfig,
}

/// A producer whose configuration can be replaced while it is running.
///
/// Cheap to clone; clones share one underlying client and all see a reload.
#[derive(Clone)]
pub struct ReloadingKafkaProducer {
    inner: Arc<Inner>,
    /// Held only for its `Drop`: when the last handle goes away, this stops
    /// the reload worker. The worker holds a `Weak<Inner>` instead of a handle,
    /// so it does not keep this alive.
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
    /// Read on every produce, write only during a swap.
    current: RwLock<Current>,
    /// Number of drains in progress, normally zero or one. Keyed produce calls
    /// wait for it to reach zero. Separate from `current` so produce calls are
    /// not blocked merely to read the pointer.
    drains_in_flight: Mutex<usize>,
    /// Signalled when `drains_in_flight` reaches zero.
    drain_done: Condvar,
    /// Serializes reloads and holds the state a swap needs.
    reload: Mutex<ReloadState>,
    /// Signalled when `reload.desired` or `reload.shutdown` changes.
    reload_wake: Condvar,
    selector: ProducerSelector,
    settings: ReloadConfig,
    generation: AtomicU64,
    /// Test-only: keyed enqueues without the ordering proof.
    #[cfg(test)]
    enqueued_during_drain: AtomicU64,
    /// Test-only: keyed enqueues with the ordering proof.
    #[cfg(test)]
    keyed_enqueues: AtomicU64,
    /// Test-only: blocks the worker inside `probe` so a test can make a push
    /// land while a rollout is provably in flight, instead of racing a sleep
    /// against a probe that finishes in microseconds.
    #[cfg(test)]
    probe_gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    /// Test-only: counts probe attempts, so a test can tell whether the worker
    /// abandoned a config or is still retrying it.
    #[cfg(test)]
    probe_attempts: AtomicU64,
    /// Test-only: parks the worker after the probe and jitter, immediately
    /// before the install decision, so a test can land a push inside the exact
    /// window the atomic re-check in `swap` guards. In production that window
    /// is the jitter sleep; the gate makes it wide deterministically where
    /// jitter makes it wide randomly.
    ///
    /// This must sit between the jitter sleep and the `reload` lock. Moving it
    /// inside the critical section would quietly weaken the test that depends
    /// on it into a duplicate of `test_revert_cancels_an_in_flight_rollout`.
    #[cfg(test)]
    swap_gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    /// Test-only: rollouts the worker has finished, however they ended, so a
    /// test can wait for the outcome to be settled instead of sleeping.
    #[cfg(test)]
    rollouts_finished: AtomicU64,
}

struct ReloadState {
    /// The config the host wants instead of the current one.
    ///
    /// Each push overwrites it; a revert clears it. The worker clones it and
    /// leaves it in place while working, so either change cancels an in-flight
    /// rollout before it can swap.
    desired: Option<KafkaConfig>,
    /// Set when every handle is dropped, to stop the worker.
    shutdown: bool,
}

impl ReloadingKafkaProducer {
    /// Builds a producer from an initial config blob.
    ///
    /// The blob is opaque to the caller: pass the bytes from options-automator
    /// straight through. `selector` says which topic and application to resolve
    /// config for.
    pub fn new(
        blob: &[u8],
        selector: ProducerSelector,
        settings: ReloadConfig,
    ) -> Result<Self, ReloadError> {
        let config = ConfigBlob::parse(blob)?.producer_config(&selector)?;
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
        let producer = Arc::new(KafkaProducer::new(config.clone())?);
        tracing::info!(
            topic = %selector.topic(),
            app = %selector.app(),
            "loaded initial producer config"
        );

        let inner = Arc::new(Inner {
            current: RwLock::new(Current { producer, config }),
            drains_in_flight: Mutex::new(0),
            drain_done: Condvar::new(),
            reload: Mutex::new(ReloadState {
                desired: None,
                shutdown: false,
            }),
            reload_wake: Condvar::new(),
            selector,
            settings,
            generation: AtomicU64::new(0),
            #[cfg(test)]
            enqueued_during_drain: AtomicU64::new(0),
            #[cfg(test)]
            keyed_enqueues: AtomicU64::new(0),
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
    /// Call when the blob changes. Unchanged blobs do nothing; invalid blobs
    /// leave the current client untouched.
    ///
    /// This parses and hands off without blocking. [`ReloadOutcome::Accepted`]
    /// means rollout started, not that the config is live; watch
    /// [`ReloadingKafkaProducer::generation`]. Broker work happens on the worker.
    pub fn push_config(&self, blob: &[u8]) -> Result<ReloadOutcome, ReloadError> {
        let config = ConfigBlob::parse(blob)?.producer_config(&self.inner.selector)?;
        Ok(self.inner.set_desired(config))
    }

    /// Number of swaps so far. Useful as a metric tag to tell which config a
    /// message was produced under.
    pub fn generation(&self) -> u64 {
        self.inner.generation.load(Ordering::SeqCst)
    }

    /// Messages accepted but not yet delivered by the live client.
    pub fn in_flight_count(&self) -> i32 {
        self.inner.current.read().producer.in_flight_count()
    }

    /// Pretends a drain is in progress, so tests can observe how produce calls
    /// behave during one without racing a real flush.
    #[cfg(test)]
    fn block_drain_for_test(&self) -> DrainGuard {
        DrainGuard::new(self.inner.clone())
    }
}

impl Inner {
    /// Records the config the host wants running and wakes the worker.
    fn set_desired(self: &Arc<Self>, config: KafkaConfig) -> ReloadOutcome {
        let mut reload = self.reload.lock();

        // Lock order is `reload` then `current`, matching `swap`.
        if config_eq(&config, &self.current.read().config) {
            // Clearing `desired` cancels any rollout back to this current config.
            reload.desired = None;
            self.reload_wake.notify_all();
            metrics::counter!(
                "arroyo.producer.config_reload_unchanged",
                "topic" => self.selector.topic().to_owned(),
            )
            .increment(1);
            return ReloadOutcome::Unchanged;
        }

        // Overwrite so only the newest config remains. An identical push leaves
        // an in-flight rollout undisturbed.
        reload.desired = Some(config);
        self.reload_wake.notify_all();

        metrics::counter!(
            "arroyo.producer.config_reload_accepted",
            "topic" => self.selector.topic().to_owned(),
        )
        .increment(1);

        ReloadOutcome::Accepted
    }

    /// Runs the reload worker until every handle is dropped.
    ///
    /// Owns everything that must not block the application: the broker probe,
    /// jitter, the swap, and draining the old client.
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

            // Done with this config, whether it went live or was abandoned.
            // Clear it so the worker can go back to sleep, unless a push
            // replaced it while we worked, in which case leave the newer one
            // for the next iteration.
            let mut reload = self.reload.lock();
            if reload
                .desired
                .as_ref()
                .is_some_and(|d| config_eq(d, &config))
            {
                reload.desired = None;
            }
        }
    }

    /// Probes a config and swaps to it, retrying until it is healthy or the
    /// host stops wanting it.
    fn reload_to(self: &Arc<Self>, config: &KafkaConfig) {
        // Build once outside the retry loop. Rebuilding would spawn fresh
        // rdkafka threads to connect-spam a dead broker on every retry.
        // Invalid client config never reaches the probe or affects the current client.
        let candidate = match KafkaProducer::new(config.clone()) {
            Ok(producer) => Arc::new(producer),
            Err(error) => {
                metrics::counter!(
                    "arroyo.producer.config_reload_rejected",
                    "topic" => self.selector.topic().to_owned(),
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
            match self.probe(&candidate) {
                Ok(()) => {
                    // `swap` re-checks that the config is still wanted itself,
                    // atomically with installing the client.
                    self.swap(candidate.clone(), config);
                    return;
                }
                Err(error) => {
                    metrics::counter!(
                        "arroyo.producer.config_reload_probe_failed",
                        "topic" => self.selector.topic().to_owned(),
                    )
                    .increment(1);
                    tracing::warn!(
                        topic = %self.selector.topic(),
                        %error,
                        "new config cannot reach a broker, staying on the running one"
                    );
                }
            }

            // Retry until healthy or superseded. An unreachable broker is as
            // likely to be a blip as a bad config, and the running producer is
            // unaffected while we wait.
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
                // The host wants something else, or we are shutting down.
                // Abandon this one; the worker loop picks up what is desired.
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
        // Metadata for the topic we produce to: one round-trip, no message
        // written, and it also catches a topic that does not exist.
        let result =
            candidate.validate_topic(crate::types::Topic::new(self.selector.topic()), timeout);

        metrics::histogram!(
            "arroyo.producer.config_reload_probe_ms",
            "topic" => self.selector.topic().to_owned(),
        )
        .record(started.elapsed().as_millis() as f64);

        result
    }

    /// Installs an already-probed client and drains the old one.
    fn swap(self: &Arc<Self>, new_producer: Arc<KafkaProducer>, config: &KafkaConfig) {
        // Jitter before claiming anything, so a fleet does not swap in unison.
        if !self.settings.jitter.is_zero() {
            let jitter = rand::random::<f64>() * self.settings.jitter.as_secs_f64();
            std::thread::sleep(Duration::from_secs_f64(jitter));
        }

        // Test-only: park here, after the jitter and before the install
        // decision, so a test can land a push inside the guarded window.
        #[cfg(test)]
        {
            let gate = self.swap_gate.lock().take();
            if let Some(gate) = gate {
                let _ = gate.recv();
            }
        }

        // After jitter, hold `reload` across the superseded check and install;
        // otherwise a push between them could install an unwanted client.
        // Lock order is `reload` then `current`, matching `set_desired`; never
        // reverse it.
        let reload = self.reload.lock();

        if superseded(&reload, config) {
            metrics::counter!(
                "arroyo.producer.config_reload_superseded",
                "topic" => self.selector.topic().to_owned(),
            )
            .increment(1);
            tracing::info!(
                topic = %self.selector.topic(),
                "config superseded before it went live, not swapping to it"
            );
            return;
        }

        // Count the drain before the swap becomes visible. A keyed produce
        // checks this counter while holding a read guard on `current`, so
        // anything that sees the new client also sees the drain and waits.
        //
        // Decremented when this guard is dropped on the drain thread, even if
        // that thread panics. Leaking a count would make every later keyed
        // produce wait out the full drain timeout, forever.
        let drain_guard = DrainGuard::new(self.clone());

        let old = {
            let mut current = self.current.write();
            std::mem::replace(
                &mut *current,
                Current {
                    producer: new_producer,
                    config: config.clone(),
                },
            )
        };

        drop(reload);

        self.generation.fetch_add(1, Ordering::SeqCst);
        metrics::counter!(
            "arroyo.producer.config_reload_applied",
            "topic" => self.selector.topic().to_owned(),
        )
        .increment(1);

        let timeout = self.settings.drain_timeout;
        let selector_topic = self.selector.topic().to_owned();

        // Drain inline on the worker to keep `drains_in_flight` at one. Thus a
        // keyed produce waits for at most one drain; unkeyed produce is unaffected.
        {
            let _drain_guard = drain_guard;

            let started = Instant::now();
            let result = old.producer.flush_for(timeout);
            let elapsed = started.elapsed();

            metrics::histogram!(
                "arroyo.producer.config_reload_drain_ms",
                "topic" => selector_topic.clone(),
            )
            .record(elapsed.as_millis() as f64);

            match result {
                Ok(()) => {
                    tracing::info!(
                        topic = %selector_topic,
                        drain_ms = elapsed.as_millis() as u64,
                        "drained producer after config reload"
                    );
                }
                Err(error) => {
                    // Drop purges queued and in-flight messages, so a timeout
                    // loses them rather than delivering them late. Their callbacks report
                    // purge errors in `arroyo.producer.produce_status`; alert on
                    // `arroyo.producer.config_reload_purged_messages`.
                    let queued = old.producer.in_flight_count();
                    metrics::counter!(
                        "arroyo.producer.config_reload_drain_timeout",
                        "topic" => selector_topic.clone(),
                    )
                    .increment(1);
                    metrics::counter!(
                        "arroyo.producer.config_reload_purged_messages",
                        "topic" => selector_topic.clone(),
                    )
                    .increment(queued.max(0) as u64);
                    tracing::error!(
                        topic = %selector_topic,
                        %error,
                        queued,
                        "producer drain timed out after config reload, queued messages will be dropped"
                    );
                }
            }

            drop(old);
        }
    }

    /// Blocks until no drain is in progress, or until the drain timeout
    /// expires. Returns `false` if it gave up waiting.
    ///
    /// On timeout we fail open: producing matters more than ordering, so the
    /// caller proceeds and we record that ordering may have been broken.
    fn wait_for_drain(&self, deadline: Instant) -> bool {
        let mut in_flight = self.drains_in_flight.lock();

        while *in_flight > 0 {
            if self
                .drain_done
                .wait_until(&mut in_flight, deadline)
                .timed_out()
            {
                metrics::counter!(
                    "arroyo.producer.config_reload_produce_fail_open",
                    "topic" => self.selector.topic().to_owned(),
                )
                .increment(1);
                tracing::warn!(
                    topic = %self.selector.topic(),
                    "keyed produce proceeding before drain finished, ordering may be broken"
                );
                return false;
            }
        }

        true
    }

    /// Records whether a keyed enqueue observed no drain under its `current`
    /// guard. Checking first or enqueueing after releasing the guard records no
    /// proof; only the deliberate timeout path may do so.
    #[cfg(test)]
    fn record_keyed_enqueue(&self, ordered: bool) {
        if ordered {
            self.keyed_enqueues.fetch_add(1, Ordering::Relaxed);
        } else {
            self.enqueued_during_drain.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Releases the drain count on drop, including during panic.
struct DrainGuard {
    inner: Arc<Inner>,
}

impl DrainGuard {
    /// Registers a drain; `Drop` guarantees the matching decrement.
    fn new(inner: Arc<Inner>) -> Self {
        *inner.drains_in_flight.lock() += 1;
        Self { inner }
    }
}

impl Drop for DrainGuard {
    fn drop(&mut self) {
        let mut in_flight = self.inner.drains_in_flight.lock();
        *in_flight -= 1;
        if *in_flight == 0 {
            self.inner.drain_done.notify_all();
        }
    }
}

impl ArroyoProducer<KafkaPayload> for ReloadingKafkaProducer {
    fn produce(
        &self,
        destination: &TopicOrPartition,
        payload: KafkaPayload,
    ) -> Result<(), ProducerError> {
        // Unkeyed messages have no ordering constraint, so they never wait for
        // a drain and go straight to whichever client is live. With
        // `ignore_key_ordering` the caller says the same holds for keyed ones.
        if payload.key().is_none() || self.inner.settings.ignore_key_ordering {
            let current = self.inner.current.read();
            return current.producer.produce(destination, payload);
        }

        // Take `current` first and hold it through the counter check and enqueue.
        // `swap` counts the drain before taking the write lock, so we either see
        // the drain and wait, or block the swap until enqueueing on the old client.
        // Checking the counter first would let a swap land before the `current`
        // read, enqueueing this message on the new client ahead of older ones.
        let deadline = Instant::now() + self.inner.settings.drain_timeout;

        loop {
            let current = self.inner.current.read();
            let draining = *self.inner.drains_in_flight.lock() > 0;

            if !draining {
                #[cfg(test)]
                self.inner.record_keyed_enqueue(true);

                // Enqueue under the read guard so `swap` cannot install first.
                return current.producer.produce(destination, payload);
            }

            // Release the read guard before waiting so the swap can proceed.
            drop(current);

            if !self.inner.wait_for_drain(deadline) {
                // Timed out. Fail open: produce against whatever is live now.
                // The only sanctioned way to enqueue a keyed message without
                // the ordering proof.
                #[cfg(test)]
                self.inner.record_keyed_enqueue(false);

                let current = self.inner.current.read();
                return current.producer.produce(destination, payload);
            }
        }
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

/// Whether shutdown, a newer push, or a revert clearing `desired` cancels this config.
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

    fn settings() -> ReloadConfig {
        ReloadConfig {
            drain_timeout: Duration::from_secs(5),
            jitter: Duration::ZERO,
            // MockCluster answers metadata, so probing stays on in tests.
            probe_timeout: Some(Duration::from_secs(5)),
            probe_retry_interval: Duration::from_millis(50),
            ignore_key_ordering: false,
        }
    }

    /// Waits for the reload worker to reach `generation`, since swaps are no
    /// longer synchronous with `push_config`.
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

    /// Blocks until the worker is inside `probe`, which it signals by taking
    /// the gate receiver. Deterministic, unlike sleeping and hoping.
    #[track_caller]
    fn wait_until_probing(producer: &ReloadingKafkaProducer) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while producer.inner.probe_gate.lock().is_some() {
            assert!(Instant::now() < deadline, "worker never reached the probe");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Asserts no swap happens within a short window.
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
        // Still usable.
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

        // Still producing on the original config.
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

        // A reachable config replaces the one stuck retrying.
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

        // Roll out "1" and hold the worker in its probe.
        producer.push_config(&blob(&servers, "1")).unwrap();
        wait_until_probing(&producer);

        // Queue a further change, then revert to the config being rolled out.
        producer.push_config(&blob(&servers, "0")).unwrap();
        producer.push_config(&blob(&servers, "1")).unwrap();

        release.send(()).unwrap();
        await_generation(&producer, 1);

        // Settles on "1" with no second swap: the "0" was replaced before the
        // worker ever took it.
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

    /// The ordering guarantee: a keyed message must never be enqueued while an
    /// older client is still draining.
    ///
    /// The test holds a drain open because MockCluster drains in microseconds,
    /// then races a keyed produce with a real swap. It fails if `produce` checks
    /// the counter before `current`, which can enqueue on the new client first.
    #[test]
    fn test_keyed_produce_never_races_a_swap() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let destination = TopicOrPartition::Topic(crate::types::Topic::new("ingest-events"));

        for round in 0..40 {
            let producer = producer(&servers);

            // Hold a drain open so the window is guaranteed to be wide.
            let held = producer.block_drain_for_test();

            // A keyed produce that must wait for that drain.
            let (tx, rx) = mpsc::channel();
            let writer = producer.clone();
            std::thread::spawn(move || {
                let _ = tx.send(writer.produce(
                    &destination,
                    KafkaPayload::new(Some(b"key".to_vec()), None, Some(b"keyed".to_vec())),
                ));
            });

            // Let it reach the wait, then swap underneath it.
            std::thread::sleep(Duration::from_millis(5));
            producer
                .push_config(&blob(&servers, &format!("{round}")))
                .unwrap();

            // Release the drain and let the produce complete.
            drop(held);
            rx.recv_timeout(Duration::from_secs(5))
                .expect("keyed produce never completed")
                .expect("keyed produce failed");

            assert_eq!(
                producer.inner.enqueued_during_drain.load(Ordering::Relaxed),
                0,
                "keyed produce landed on a client while an older one was draining"
            );
            assert!(
                producer.inner.keyed_enqueues.load(Ordering::Relaxed) > 0,
                "no keyed enqueues were recorded"
            );
        }
    }

    /// Unkeyed produce must stay non-blocking even under constant swapping,
    /// since it has no ordering constraint to preserve.
    #[test]
    fn test_unkeyed_produce_never_blocks_under_swaps() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);
        let destination = TopicOrPartition::Topic(crate::types::Topic::new("ingest-events"));

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worst = Arc::new(Mutex::new(Duration::ZERO));

        let writer = {
            let producer = producer.clone();
            let stop = stop.clone();
            let worst = worst.clone();
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let started = Instant::now();
                    producer
                        .produce(
                            &destination,
                            KafkaPayload::new(None, None, Some(b"payload".to_vec())),
                        )
                        .unwrap();
                    let elapsed = started.elapsed();

                    let mut worst = worst.lock();
                    if elapsed > *worst {
                        *worst = elapsed;
                    }
                }
            })
        };

        for i in 0..25 {
            producer
                .push_config(&blob(&servers, &format!("{}", i % 2)))
                .unwrap();
            std::thread::sleep(Duration::from_millis(2));
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        writer.join().unwrap();

        // Nowhere near drain_timeout (5s); it never waited on a drain.
        let worst = *worst.lock();
        assert!(
            worst < Duration::from_millis(500),
            "unkeyed produce blocked for {worst:?}, it should never wait for a drain"
        );
    }

    /// A panicking drain must not strand the counter, which would make every
    /// later keyed produce wait out the full timeout.
    #[test]
    fn test_drain_guard_releases_on_panic() {
        let cluster = MockCluster::new(1).unwrap();
        let producer = producer(&cluster.bootstrap_servers());

        let inner = producer.inner.clone();
        std::thread::spawn(move || {
            let _guard = DrainGuard::new(inner);
            panic!("drain exploded");
        })
        .join()
        .unwrap_err();

        assert_eq!(*producer.inner.drains_in_flight.lock(), 0);

        // Keyed produce still proceeds immediately.
        let started = Instant::now();
        producer
            .produce(
                &TopicOrPartition::Topic(crate::types::Topic::new("ingest-events")),
                KafkaPayload::new(Some(b"key".to_vec()), None, Some(b"payload".to_vec())),
            )
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
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

    /// A push that reverts to the running config must cancel a rollout that is
    /// already in flight.
    ///
    /// The worker must clone `desired` and leave it in place so a revert can
    /// clear it before the swap. The probe is gated instead of raced against a
    /// sleep because MockCluster answers in microseconds and this path requires
    /// a successful probe.
    #[test]
    fn test_revert_cancels_an_in_flight_rollout() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);

        // Arm the gate before pushing, so the worker cannot get past the probe
        // before the revert lands.
        let (release, gate) = mpsc::channel();
        *producer.inner.probe_gate.lock() = Some(gate);

        // Reachable config: this probe will succeed once released.
        producer.push_config(&blob(&servers, "9")).unwrap();

        // The worker is now inside the probe, working on this config while it
        // remains recorded as the desired one.
        wait_until_probing(&producer);

        // The rollout is provably in flight. Revert to what is running.
        assert_eq!(
            producer.push_config(&blob(&servers, "all")).unwrap(),
            ReloadOutcome::Unchanged
        );

        // Let the probe finish successfully.
        release.send(()).unwrap();

        // The reverted-away config must not go live.
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
    /// cancel the swap.
    ///
    /// The superseded check and install must be atomic after jitter, or a revert
    /// in between is lost. This test sets jitter to zero and parks the worker at
    /// the gate between sleep and lock acquisition, so the revert cannot lose a
    /// wall-clock race.
    #[test]
    fn test_revert_between_probe_and_install_cancels_the_swap() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);

        // Arm before pushing, so the worker cannot get past the gate first.
        let (release, gate) = mpsc::channel();
        *producer.inner.swap_gate.lock() = Some(gate);

        producer.push_config(&blob(&servers, "9")).unwrap();

        // Worker parked: probe passed, install not reached.
        let deadline = Instant::now() + Duration::from_secs(5);
        while producer.inner.swap_gate.lock().is_some() {
            assert!(Instant::now() < deadline, "worker never reached the swap");
            std::thread::sleep(Duration::from_millis(5));
        }

        // The revert lands inside the window. `Unchanged` is guaranteed here,
        // not raced for: the worker is parked before the install.
        assert_eq!(
            producer.push_config(&blob(&servers, "all")).unwrap(),
            ReloadOutcome::Unchanged
        );

        release.send(()).unwrap();

        // Wait until the rollout ended one way or the other, then check which.
        // Both a correct and a broken implementation reach this point, so the
        // generation assertion below is what tells them apart.
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

    /// After a revert, the worker must stop retrying the reverted-away config.
    ///
    /// This pins the liveness half of the fix. If the supersede check only
    /// looked for a *newer* config rather than comparing against `desired`, a
    /// revert would leave nothing to compare against and the worker would keep
    /// probing a config the host rolled back. If that broker later recovers,
    /// the config goes live long after the fact.
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

        // Unreachable broker, so the worker keeps retrying.
        producer.push_config(&blob("127.0.0.1:1", "9")).unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        while producer.inner.probe_attempts.load(Ordering::Relaxed) < 2 {
            assert!(Instant::now() < deadline, "worker never retried the probe");
            std::thread::sleep(Duration::from_millis(10));
        }

        // Revert to the running config.
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
        // `reload_to` exists for: without it the worker takes the duplicate
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

    #[test]
    fn test_unkeyed_produce_does_not_wait_for_drain() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);
        let destination = TopicOrPartition::Topic(crate::types::Topic::new("ingest-events"));

        // Stand in for a drain in progress.
        let held = producer.block_drain_for_test();

        let (tx, rx) = mpsc::channel();
        let unkeyed = producer.clone();
        let dest = destination;
        std::thread::spawn(move || {
            let result = unkeyed.produce(
                &dest,
                KafkaPayload::new(None, None, Some(b"unkeyed".to_vec())),
            );
            let _ = tx.send(result);
        });

        // Returns while the drain is still running.
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("unkeyed produce blocked on the drain");
        assert!(result.is_ok());
        drop(held);
    }

    #[test]
    fn test_keyed_produce_waits_for_drain() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = producer(&servers);
        let destination = TopicOrPartition::Topic(crate::types::Topic::new("ingest-events"));

        let held = producer.block_drain_for_test();

        let (tx, rx) = mpsc::channel();
        let keyed = producer.clone();
        let dest = destination;
        std::thread::spawn(move || {
            let result = keyed.produce(
                &dest,
                KafkaPayload::new(Some(b"key".to_vec()), None, Some(b"keyed".to_vec())),
            );
            let _ = tx.send(result);
        });

        // Blocked while the drain is in progress.
        assert!(rx.recv_timeout(Duration::from_millis(300)).is_err());

        drop(held);

        // Proceeds once the drain finishes.
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("keyed produce did not resume after the drain");
        assert!(result.is_ok());
    }

    #[test]
    fn test_keyed_produce_does_not_wait_when_ordering_is_ignored() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = ReloadingKafkaProducer::new(
            &blob(&servers, "all"),
            selector(),
            ReloadConfig {
                // Long enough that waiting would show up as a timeout below.
                drain_timeout: Duration::from_secs(30),
                ignore_key_ordering: true,
                ..settings()
            },
        )
        .unwrap();
        let destination = TopicOrPartition::Topic(crate::types::Topic::new("ingest-events"));

        let held = producer.block_drain_for_test();

        let (tx, rx) = mpsc::channel();
        let keyed = producer.clone();
        let dest = destination;
        std::thread::spawn(move || {
            let result = keyed.produce(
                &dest,
                KafkaPayload::new(Some(b"key".to_vec()), None, Some(b"keyed".to_vec())),
            );
            let _ = tx.send(result);
        });

        // Returns while the drain is still running, like an unkeyed produce.
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("keyed produce waited for the drain despite ignore_key_ordering");
        assert!(result.is_ok());
        drop(held);
    }

    #[test]
    fn test_keyed_produce_fails_open_when_drain_is_slow() {
        let cluster = MockCluster::new(1).unwrap();
        let servers = cluster.bootstrap_servers();
        let producer = ReloadingKafkaProducer::new(
            &blob(&servers, "all"),
            selector(),
            ReloadConfig {
                drain_timeout: Duration::from_millis(200),
                ..settings()
            },
        )
        .unwrap();
        let destination = TopicOrPartition::Topic(crate::types::Topic::new("ingest-events"));

        // A drain that outlasts drain_timeout.
        let held = producer.block_drain_for_test();

        let started = Instant::now();
        let result = producer.produce(
            &destination,
            KafkaPayload::new(Some(b"key".to_vec()), None, Some(b"keyed".to_vec())),
        );

        // Waited for the timeout, then produced anyway rather than erroring.
        assert!(result.is_ok());
        assert!(started.elapsed() >= Duration::from_millis(200));
        drop(held);
    }
}
