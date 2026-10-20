use chrono::{DateTime, Utc};
use rdkafka::message::{BorrowedMessage, Message as RdkafkaMessage};
use std::collections::HashMap;

use crate::backends::kafka::types::KafkaPayload;
use crate::types::{Partition, Topic};

/// Metadata about the original Kafka message — used for offset tracking
/// and latency metrics. Separated from the raw payload to avoid conflation.
#[derive(Debug, Clone)]
pub struct MessageMetadata {
    pub partition: Partition,
    pub offset: u64,
    pub timestamp: DateTime<Utc>,
}

/// A message envelope that carries context through a pull-based pipeline.
///
/// Four concerns, four fields:
///   - `payload` — the current transformed data (changes at each stage)
///   - `metadata` — partition/offset/timestamp of the originating message
///   - `raw` — original Kafka bytes for DLQ
///   - `offsets` — the offsets this envelope commits
pub struct PipelineEnvelope<T> {
    pub payload: T,
    pub metadata: MessageMetadata,
    pub raw: KafkaPayload,
    pub offsets: HashMap<Partition, u64>,
}

impl<T> PipelineEnvelope<T> {
    /// Offsets are explicit, not derived from `metadata`, so a stage after
    /// a batch cannot silently narrow the commit to one partition.
    pub fn new(
        payload: T,
        metadata: MessageMetadata,
        raw: KafkaPayload,
        offsets: HashMap<Partition, u64>,
    ) -> Self {
        Self {
            payload,
            metadata,
            raw,
            offsets,
        }
    }

    /// Transform the payload, preserving metadata, raw, and offsets.
    pub fn map_payload<U>(self, f: impl FnOnce(T) -> U) -> PipelineEnvelope<U> {
        PipelineEnvelope {
            payload: f(self.payload),
            metadata: self.metadata,
            raw: self.raw,
            offsets: self.offsets,
        }
    }

    /// Transform the payload with a fallible function.
    pub fn try_map_payload<U, E>(
        self,
        f: impl FnOnce(T) -> Result<U, E>,
    ) -> Result<PipelineEnvelope<U>, E> {
        Ok(PipelineEnvelope {
            payload: f(self.payload)?,
            metadata: self.metadata,
            raw: self.raw,
            offsets: self.offsets,
        })
    }
}

impl PipelineEnvelope<KafkaPayload> {
    /// Create an envelope directly from an rdkafka BorrowedMessage.
    /// Copies key, headers, payload bytes out of rdkafka's internal buffer
    /// and extracts the broker timestamp.
    pub fn from_kafka(msg: &BorrowedMessage<'_>) -> Self {
        let topic = Topic::new(msg.topic());
        let partition = Partition::new(topic, msg.partition() as u16);
        let time_millis = msg.timestamp().to_millis().unwrap_or(0);
        let timestamp =
            DateTime::from_timestamp_millis(time_millis).unwrap_or(DateTime::<Utc>::MIN_UTC);

        let kafka_payload = KafkaPayload::new(
            msg.key().map(|k| k.to_vec()),
            msg.headers().map(|h| h.into()),
            msg.payload().map(|p| p.to_vec()),
        );

        let metadata = MessageMetadata {
            partition,
            offset: msg.offset() as u64,
            timestamp,
        };

        Self {
            raw: kafka_payload.clone(),
            payload: kafka_payload,
            offsets: HashMap::from([(metadata.partition, metadata.offset)]),
            metadata,
        }
    }
}
