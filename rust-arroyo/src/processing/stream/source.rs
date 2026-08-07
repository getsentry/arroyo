use std::collections::HashMap;
use std::pin::Pin;

use futures::stream::Stream;
use futures::StreamExt;
use rdkafka::config::ClientConfig as RdKafkaConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::TopicPartitionList;
use tokio_util::sync::CancellationToken;

use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::kafka_poll_error_is_recoverable;
use crate::backends::kafka::types::KafkaPayload;
use super::offset_tracker::OffsetCommitter;
use crate::types::{Partition, Topic};

use super::pipeline_envelope::PipelineEnvelope;
use super::stage::StageResult;

/// Trait for pipeline sources. Provides a stream of raw Kafka payloads,
/// an offset committer, and graceful shutdown.
pub trait PullSource: Send + Sync {
    fn stream(&self) -> Pin<Box<dyn Stream<Item = StageResult<KafkaPayload>> + '_>>;
    fn committer(&self) -> &dyn OffsetCommitter;
    fn shutdown(&self);
}

/// A Kafka consumer source that produces a Stream of StageResult<KafkaPayload>.
///
/// Supports graceful shutdown via a CancellationToken — calling shutdown()
/// terminates the stream, allowing the pipeline to drain and commit offsets.
pub struct KafkaSource {
    consumer: StreamConsumer,
    cancel: CancellationToken,
}

impl KafkaSource {
    pub fn new(config: KafkaConfig, topics: &[Topic]) -> Self {
        let mut rdkafka_config: RdKafkaConfig = config.into();
        let consumer: StreamConsumer = rdkafka_config
            .set_log_level(rdkafka::config::RDKafkaLogLevel::Warning)
            .create()
            .expect("Failed to create consumer");

        let topic_strs: Vec<&str> = topics.iter().map(|t| t.as_str()).collect();
        consumer
            .subscribe(&topic_strs)
            .expect("Failed to subscribe");

        Self {
            consumer,
            cancel: CancellationToken::new(),
        }
    }
}

impl PullSource for KafkaSource {
    fn stream(&self) -> Pin<Box<dyn Stream<Item = StageResult<KafkaPayload>> + '_>> {
        Box::pin(
            self.consumer
                .stream()
                .take_until(self.cancel.cancelled())
                .filter_map(|result| {
                    futures::future::ready(match result {
                        Ok(msg) => Some(StageResult::Emit(PipelineEnvelope::from_kafka(&msg))),
                        Err(e) if kafka_poll_error_is_recoverable(&e) => {
                            tracing::warn!("Recoverable Kafka error, skipping: {}", e);
                            None
                        }
                        Err(e) => {
                            tracing::error!("Fatal Kafka error: {}", e);
                            Some(StageResult::Fail(Box::new(e)))
                        }
                    })
                }),
        )
    }

    fn committer(&self) -> &dyn OffsetCommitter {
        self
    }

    fn shutdown(&self) {
        self.cancel.cancel();
    }
}

impl OffsetCommitter for KafkaSource {
    fn commit_offsets(
        &self,
        positions: &HashMap<Partition, u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let mut tpl = TopicPartitionList::new();
        for (partition, offset) in positions {
            tpl.add_partition_offset(
                partition.topic.as_str(),
                partition.index as i32,
                rdkafka::Offset::Offset(*offset as i64),
            )
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        }
        // Async commit matches the existing push model's behavior. The broker
        // may not ack before we clear tracked offsets, but this is acceptable —
        // worst case on crash is re-processing already-committed messages.
        self.consumer
            .commit(&tpl, CommitMode::Async)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)
    }
}
