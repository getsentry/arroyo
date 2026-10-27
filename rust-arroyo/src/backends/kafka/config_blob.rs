//! Parsing of the central Kafka configmap blob.
//!
//! The host passes bytes from options-automator at startup and on changes;
//! Arroyo never reads the blob from disk. Arroyo owns this Sentry-specific
//! schema so hosts can treat it as opaque.

use std::collections::HashMap;

use serde::Deserialize;
use thiserror::Error;

use super::config::KafkaConfig;

use crate::types::Topic;

/// Failure to resolve a config blob. Rejection leaves the current config intact.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum ConfigBlobError {
    #[error("config blob is not valid JSON")]
    Malformed(#[from] serde_json::Error),

    #[error("config blob has no entry for topic {topic:?}")]
    UnknownTopic { topic: String },

    #[error("topic {topic:?} refers to undefined cluster {cluster:?}")]
    UnknownCluster { topic: String, cluster: String },

    #[error("environment variable {variable:?} for config key {key:?} is not set")]
    MissingEnvVar { key: String, variable: String },

    #[error("expected 1 physical topic backend, instead found {num_topics:?} backends")]
    InvalidPhysicalTopic { num_topics: usize },
}

/// The whole configmap, covering every cluster and topic in a region.
#[derive(Debug, Clone, Deserialize)]
pub struct ConfigBlob {
    #[serde(default)]
    clusters: HashMap<String, ClusterEntry>,
    #[serde(default)]
    topics: HashMap<String, LogicalTopicEntry>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ClusterEntry {
    /// Config applied to producers and consumers alike.
    #[serde(default)]
    config: HashMap<String, String>,
    /// Config mapping keys to environment variables, keeping secrets out of the blob.
    #[serde(default)]
    env_config: HashMap<String, String>,
    #[serde(default)]
    producer_config: HashMap<String, String>,
    #[serde(default)]
    producer_env_config: HashMap<String, String>,
}

/// Represents a logical topic, which maps to 1 physical topic.
/// Physical topics are abstracted entirely away from clients, so clients
/// only have to reason about producing to/consuming from logical topics.
/// TODO: Update the relationship of logical to physical topics to be 1:n.
#[derive(Debug, Clone, Default, Deserialize)]
struct LogicalTopicEntry {
    #[serde(default)]
    producer_config: HashMap<String, String>,
    /// Per-application overrides, keyed by application name such as `relay`.
    #[serde(default)]
    producer_config_overrides: HashMap<String, HashMap<String, String>>,
    /// The physical topic which this logical topic maps to.
    /// Since we only support a 1:1 relation for logical to physical topics,
    /// this cannot have more than one entry and is only a Vec to be forwards-compatible
    /// with when support for N physical topics is added.
    #[serde(default)]
    physical_topics: Vec<PhysicalTopicEntry>,
    /// Optional override for the physical topic backend used by producers.
    /// Can be used to cut over producers to a new physical topic backend,
    /// while still having consumers connect to the old physical topic backend.
    /// This helps preserve message ordering.
    #[serde(default)]
    producer_physical_topic_override: Option<Vec<PhysicalTopicEntry>>,
}

/// Represents an actual Kafka topic on a broker (aka a physical topic).
/// Logical topics (which clients work in terms of) map to one physical topic, but will
/// be updated to support 1:n logical-to-physical topic mappings in the future.
#[derive(Debug, Clone, Default, Deserialize)]
struct PhysicalTopicEntry {
    /// Which cluster this physical topic lives on.
    #[serde(default)]
    cluster: String,
    /// Optional topic name override. By default the logical topic name will be used.
    #[serde(default)]
    topic_name_override: Option<String>,
}

/// Helper function for extracting the physical topic from a Vec of physical topics.
fn get_physical_topic(
    topics: &[PhysicalTopicEntry],
) -> Result<&PhysicalTopicEntry, ConfigBlobError> {
    match topics.len() {
        1 => Ok(&topics[0]),
        _ => Err(ConfigBlobError::InvalidPhysicalTopic {
            num_topics: topics.len(),
        }),
    }
}

/// The same as `KafkaConfig`, but includes the physical topic name that the client
/// who uses this config should produce to/consume from.
#[derive(Debug, Clone)]
pub struct ResolvedClientConfig {
    pub config: KafkaConfig,
    pub physical_topic: Topic,
}

/// Where to read environment variables from when resolving `env_config`.
///
/// Exists so tests can resolve a blob without touching process state.
pub trait EnvSource {
    fn get(&self, key: &str) -> Option<String>;
}

/// Reads from the real process environment.
pub struct ProcessEnv;

impl EnvSource for ProcessEnv {
    fn get(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }
}

impl EnvSource for HashMap<String, String> {
    fn get(&self, key: &str) -> Option<String> {
        HashMap::get(self, key).cloned()
    }
}

/// Identifies which producer config to pull out of a blob.
#[derive(Debug, Clone)]
pub struct ProducerSelector {
    topic: String,
    app: String,
}

impl ProducerSelector {
    /// Selects the producer config for a logical topic, as seen by `app`.
    ///
    /// `app` decides which `producer_config_overrides` entry applies.
    pub fn new(topic: impl Into<String>, app: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            app: app.into(),
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn app(&self) -> &str {
        &self.app
    }
}

impl ConfigBlob {
    /// Parses a blob. The bytes come from the host application verbatim.
    pub fn parse(blob: &[u8]) -> Result<Self, ConfigBlobError> {
        Ok(serde_json::from_slice(blob)?)
    }

    /// Resolves the producer config for one logical topic, reading secrets from the
    /// process environment.
    pub fn producer_config(
        &self,
        selector: &ProducerSelector,
    ) -> Result<ResolvedClientConfig, ConfigBlobError> {
        self.producer_config_with_env(selector, &ProcessEnv)
    }

    /// Same as [`ConfigBlob::producer_config`], with an explicit environment.
    pub fn producer_config_with_env(
        &self,
        selector: &ProducerSelector,
        env: &impl EnvSource,
    ) -> Result<ResolvedClientConfig, ConfigBlobError> {
        let topic_name = &selector.topic;
        let topic = self
            .topics
            .get(topic_name)
            .ok_or_else(|| ConfigBlobError::UnknownTopic {
                topic: topic_name.clone(),
            })?;
        // Uses the producer backend override if one exists,
        // otherwise the standard physical topic backends
        let physical_topic = get_physical_topic(
            topic
                .producer_physical_topic_override
                .as_ref()
                .unwrap_or(&topic.physical_topics),
        )?;

        // Later sources win: cluster-wide settings first, then producer
        // settings, then the topic's own, then the per-app override.
        let mut params = HashMap::new();
        let cluster_name = &physical_topic.cluster;
        let cluster =
            self.clusters
                .get(cluster_name)
                .ok_or_else(|| ConfigBlobError::UnknownCluster {
                    topic: topic_name.clone(),
                    cluster: cluster_name.clone(),
                })?;

        params.extend(cluster.config.clone());
        resolve_env_into(&mut params, &cluster.env_config, env)?;
        params.extend(cluster.producer_config.clone());
        resolve_env_into(&mut params, &cluster.producer_env_config, env)?;
        params.extend(topic.producer_config.clone());
        if let Some(overrides) = topic.producer_config_overrides.get(&selector.app) {
            params.extend(overrides.clone());
        }

        Ok(ResolvedClientConfig {
            // `bootstrap.servers` is just another key in the blob, so hand the
            // whole map over as overrides rather than splitting it out.
            config: KafkaConfig::new_producer_config(Vec::new(), Some(params)),
            physical_topic: Topic::new(
                physical_topic
                    .topic_name_override
                    .as_deref()
                    .unwrap_or(topic_name),
            ),
        })
    }
}

fn resolve_env_into(
    params: &mut HashMap<String, String>,
    env_config: &HashMap<String, String>,
    env: &impl EnvSource,
) -> Result<(), ConfigBlobError> {
    for (key, variable) in env_config {
        // An unset variable is an error, not an empty value that builds a broken client.
        let value = env
            .get(variable)
            .ok_or_else(|| ConfigBlobError::MissingEnvVar {
                key: key.clone(),
                variable: variable.clone(),
            })?;
        params.insert(key.clone(), value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLOB: &[u8] = br#"{
        "clusters": {
            "events": {
                "config": {"bootstrap.servers": "broker-1:9092"},
                "env_config": {"sasl.username": "KAFKA_USERNAME"},
                "producer_config": {"acks": "all"}
            },
            "events-2": {
                "config": {"bootstrap.servers": "broker-2:9092"},
                "env_config": {"sasl.username": "KAFKA_USERNAME"},
                "producer_config": {"acks": "all"}
            }
        },
        "topics": {
            "ingest-events": {
                "physical_topics": [{"cluster": "events"}],
                "producer_config": {"compression.type": "lz4"},
                "producer_config_overrides": {"relay": {"acks": "1"}}
            },
            "ingest-spans": {
                "physical_topics": [{"cluster": "events", "topic_name_override": "ingest-spans-v2"}]
            },
            "ingest-multiple": {
                "physical_topics": [{"cluster": "events"}, {"cluster": "events-2"}]
            },
            "orphan-topic": {"physical_topics": [{"cluster": "does-not-exist"}]}
        }
    }"#;

    fn env() -> HashMap<String, String> {
        HashMap::from([("KAFKA_USERNAME".to_string(), "ingest".to_string())])
    }

    fn resolve(topic: &str, app: &str) -> Result<KafkaConfig, ConfigBlobError> {
        resolve_full(topic, app).map(|resolved| resolved.config)
    }

    fn resolve_full(topic: &str, app: &str) -> Result<ResolvedClientConfig, ConfigBlobError> {
        ConfigBlob::parse(BLOB)
            .unwrap()
            .producer_config_with_env(&ProducerSelector::new(topic, app), &env())
    }

    #[test]
    fn test_physical_topic_defaults_to_logical_name() {
        let resolved = resolve_full("ingest-events", "relay").unwrap();
        assert_eq!(resolved.physical_topic, Topic::new("ingest-events"));
    }

    #[test]
    fn test_physical_topic_override_stays_out_of_kafka_config() {
        let resolved = resolve_full("ingest-spans", "relay").unwrap();
        assert_eq!(resolved.physical_topic, Topic::new("ingest-spans-v2"));
        assert!(!resolved
            .config
            .config_map()
            .values()
            .any(|value| value == "ingest-spans-v2"));
    }

    #[test]
    fn test_layers_cluster_topic_and_app_config() {
        let config = resolve("ingest-events", "relay").unwrap();

        assert_eq!(
            config.get_config_value("bootstrap.servers"),
            Some(&"broker-1:9092".to_string())
        );
        assert_eq!(
            config.get_config_value("compression.type"),
            Some(&"lz4".to_string())
        );
        // The per-app override beats the cluster-wide value.
        assert_eq!(config.get_config_value("acks"), Some(&"1".to_string()));
    }

    #[test]
    fn test_app_without_override_keeps_cluster_value() {
        let config = resolve("ingest-events", "snuba").unwrap();
        assert_eq!(config.get_config_value("acks"), Some(&"all".to_string()));
    }

    #[test]
    fn test_env_config_is_resolved_from_environment() {
        let config = resolve("ingest-events", "relay").unwrap();
        assert_eq!(
            config.get_config_value("sasl.username"),
            Some(&"ingest".to_string())
        );
    }

    #[test]
    fn test_missing_env_var_is_rejected() {
        let result = ConfigBlob::parse(BLOB).unwrap().producer_config_with_env(
            &ProducerSelector::new("ingest-events", "relay"),
            &HashMap::new(),
        );
        assert!(matches!(
            result,
            Err(ConfigBlobError::MissingEnvVar { variable, .. }) if variable == "KAFKA_USERNAME"
        ));
    }

    #[test]
    fn test_unknown_topic_is_rejected() {
        assert!(matches!(
            resolve("no-such-topic", "relay"),
            Err(ConfigBlobError::UnknownTopic { .. })
        ));
    }

    #[test]
    fn test_unknown_cluster_is_rejected() {
        assert!(matches!(
            resolve("orphan-topic", "relay"),
            Err(ConfigBlobError::UnknownCluster { .. })
        ));
    }

    #[test]
    fn test_malformed_blob_is_rejected() {
        assert!(matches!(
            ConfigBlob::parse(b"not json"),
            Err(ConfigBlobError::Malformed(..))
        ));
    }

    #[test]
    fn test_unknown_fields_are_tolerated() {
        // Consumer keys and future additions must not break producers.
        let blob = br#"{
            "clusters": {"foo": {}},
            "topics": {"t": {"physical_topics": [{"cluster": "foo"}], "consumer_config": {"max.poll.interval.ms": "1"}, "future": 1}},
            "unrelated": {}
        }"#;
        assert!(ConfigBlob::parse(blob)
            .unwrap()
            .producer_config_with_env(&ProducerSelector::new("t", "relay"), &env())
            .is_ok());
    }
}
