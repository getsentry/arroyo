use std::sync::Arc;

use chrono::{DateTime, Utc};
use rdkafka::message::{BorrowedMessage, Message as RdkafkaMessage};

use crate::backends::kafka::types::KafkaPayload;
use crate::types::{BrokerMessage, Partition, Topic};

/// A message envelope that carries context through a pull-based pipeline.
///
/// `T` is the current payload type (changes at each transform stage).
/// The origin is always the original Kafka broker message — preserved
/// for offset tracking and DLQ.
pub struct Envelope<T> {
    pub payload: T,
    pub origin: Arc<BrokerMessage<KafkaPayload>>,
}

impl<T> Envelope<T> {
    pub fn new(payload: T, origin: Arc<BrokerMessage<KafkaPayload>>) -> Self {
        Self { payload, origin }
    }

    pub fn partition(&self) -> Partition {
        self.origin.partition
    }

    pub fn offset(&self) -> u64 {
        self.origin.offset
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.origin.timestamp
    }

    /// Transform the payload, preserving the origin context.
    pub fn map_payload<U>(self, f: impl FnOnce(T) -> U) -> Envelope<U> {
        Envelope {
            payload: f(self.payload),
            origin: self.origin,
        }
    }

    /// Transform the payload with a fallible function.
    pub fn try_map_payload<U, E>(
        self,
        f: impl FnOnce(T) -> Result<U, E>,
    ) -> Result<Envelope<U>, E> {
        Ok(Envelope {
            payload: f(self.payload)?,
            origin: self.origin,
        })
    }
}

impl Envelope<KafkaPayload> {
    /// Create an envelope directly from an rdkafka BorrowedMessage.
    /// Copies key, headers, payload bytes out of rdkafka's internal buffer
    /// and extracts the broker timestamp.
    pub fn from_kafka(msg: &BorrowedMessage<'_>) -> Self {
        let topic = Topic::new(msg.topic());
        let partition = Partition::new(topic, msg.partition() as u16);
        let time_millis = msg.timestamp().to_millis().unwrap_or(0);

        let kafka_payload = KafkaPayload::new(
            msg.key().map(|k| k.to_vec()),
            msg.headers().map(|h| h.into()),
            msg.payload().map(|p| p.to_vec()),
        );

        let broker_msg = BrokerMessage::new(
            kafka_payload.clone(),
            partition,
            msg.offset() as u64,
            DateTime::from_timestamp_millis(time_millis).unwrap_or(DateTime::<Utc>::MIN_UTC),
        );

        Self {
            payload: kafka_payload,
            origin: Arc::new(broker_msg),
        }
    }

    /// Create an envelope from an existing BrokerMessage.
    pub fn from_broker_message(msg: BrokerMessage<KafkaPayload>) -> Self {
        let payload = msg.payload.clone();
        Self {
            payload,
            origin: Arc::new(msg),
        }
    }
}
