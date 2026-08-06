use std::collections::HashMap;

use futures::stream::Stream;
use futures::StreamExt;
use rdkafka::config::ClientConfig as RdKafkaConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::TopicPartitionList;

use crate::backends::kafka::config::KafkaConfig;
use crate::processing::strategies::offset_tracker::OffsetCommitter;
use crate::types::{Partition, Topic};

use super::pipeline_envelope::PipelineEnvelope;
use super::stage::StageResult;

/// A Kafka consumer source that produces a Stream of StageResult<KafkaPayload>.
///
/// Also handles offset committing — both stream() and commit() borrow
/// &self on the underlying StreamConsumer, which rdkafka allows.
pub struct KafkaSource {
    consumer: StreamConsumer,
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

        Self { consumer }
    }

    /// Returns a Stream of StageResult<KafkaPayload>.
    pub fn stream(
        &self,
    ) -> impl Stream<Item = StageResult<crate::backends::kafka::types::KafkaPayload>> + '_ {
        self.consumer.stream().filter_map(|result| {
            futures::future::ready(match result {
                Ok(msg) => Some(StageResult::Emit(PipelineEnvelope::from_kafka(&msg))),
                Err(e) => {
                    tracing::error!("Kafka consumer error: {}", e);
                    None
                }
            })
        })
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
        self.consumer
            .commit(&tpl, CommitMode::Async)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)
    }
}
