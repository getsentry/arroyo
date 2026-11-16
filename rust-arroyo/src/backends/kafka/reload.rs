//! Config reload pieces that do not depend on which kind of client is reloaded.

use std::time::{Duration, Instant};

use rdkafka::error::KafkaError;

use super::config_blob::{ConfigBlobError, ResolvedClientConfig};

/// Settings for how reloads are carried out.
#[derive(Debug, Clone)]
pub struct ReloadConfig {
    /// How long to flush the old client before dropping it and purging what
    /// remains. Produce calls buffer during this time.
    pub drain_timeout: Duration,
    /// Maximum messages held while the old client drains. Produce calls fail
    /// with `QueueFull` once this is reached, like a full rdkafka queue, so
    /// callers can back off and retry. Keep it below
    /// `queue.buffering.max.messages`.
    pub max_buffered_messages: usize,
    /// Upper bound on the random delay before a swap. Spreads swaps across a
    /// fleet so a config change does not stall every pod at once.
    pub jitter: Duration,
    /// Deadline for one attempt at reaching a broker with the new client.
    ///
    /// A client that fails is never installed: the worker retries every
    /// `probe_retry_interval` until it passes or the config is superseded. So
    /// this bounds a single round trip, not the rollout.
    pub probe_timeout: Duration,
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
            probe_timeout: Duration::from_secs(5),
            probe_retry_interval: Duration::from_secs(5),
            ignore_key_ordering: false,
        }
    }
}

/// What a call to `push_config` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadOutcome {
    /// The valid, changed config was handed to the worker. It is not live yet;
    /// watch the client's `generation` for the swap.
    Accepted,
    /// The blob parsed, but produced the same config as the running client.
    /// Any config queued but not yet applied is dropped.
    Unchanged,
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

pub(super) struct ReloadState {
    /// Desired config. A push overwrites it and a revert clears it, which can
    /// cancel an in-flight rollout.
    pub(super) desired: Option<ResolvedClientConfig>,
    /// When `desired` was last set, to measure how long a config took to go
    /// live. Reset on overwrite, so it always belongs to the current `desired`.
    pub(super) desired_since: Option<Instant>,
    /// Set when every handle is dropped, to stop the worker.
    pub(super) shutdown: bool,
}

/// Metric tag for why a pushed blob was rejected. Low cardinality on purpose:
/// the offending topic or variable goes in the log line, not the tag.
pub(super) fn blob_error_reason(error: &ConfigBlobError) -> &'static str {
    match error {
        ConfigBlobError::Malformed(_) => "malformed",
        ConfigBlobError::UnknownTopic { .. } => "unknown_topic",
        ConfigBlobError::UnknownCluster { .. } => "unknown_cluster",
        ConfigBlobError::MissingEnvVar { .. } => "missing_env_var",
        ConfigBlobError::InvalidPhysicalTopic { .. } => "invalid_physical_topic",
    }
}

pub(super) fn superseded(reload: &ReloadState, config: &ResolvedClientConfig) -> bool {
    reload.shutdown
        || !reload
            .desired
            .as_ref()
            .is_some_and(|desired| config_eq(desired, config))
}

/// Compares the parameter map and the physical topic, so a blob that only
/// moves the topic still counts as a change. `offset_reset_config` is
/// consumer-only and never set by the producer paths, so it cannot differ here.
pub(super) fn config_eq(left: &ResolvedClientConfig, right: &ResolvedClientConfig) -> bool {
    left.physical_topic == right.physical_topic
        && left.config.config_map() == right.config.config_map()
}
