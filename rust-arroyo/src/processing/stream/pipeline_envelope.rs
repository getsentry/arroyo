use std::sync::Arc;

use chrono::{DateTime, Utc};
use rdkafka::message::{BorrowedMessage, Message as RdkafkaMessage};

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
/// Three concerns, three fields:
///   - `payload` — the current transformed data (changes at each stage)
///   - `metadata` — partition/offset/timestamp for commit tracking
///   - `raw` — original Kafka bytes for DLQ
pub struct PipelineEnvelope<T> {
    pub payload: T,
    pub metadata: MessageMetadata,
    pub raw: Arc<KafkaPayload>,
}

impl<T> PipelineEnvelope<T> {
    pub fn new(payload: T, metadata: MessageMetadata, raw: Arc<KafkaPayload>) -> Self {
        Self {
            payload,
            metadata,
            raw,
        }
    }

    /// Transform the payload, preserving metadata and raw.
    pub fn map_payload<U>(self, f: impl FnOnce(T) -> U) -> PipelineEnvelope<U> {
        PipelineEnvelope {
            payload: f(self.payload),
            metadata: self.metadata,
            raw: self.raw,
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
            payload: kafka_payload.clone(),
            metadata,
            raw: Arc::new(kafka_payload),
        }
    }
}
