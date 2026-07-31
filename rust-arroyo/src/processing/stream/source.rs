use futures::stream::Stream;
use futures::StreamExt;
use rdkafka::config::ClientConfig as RdKafkaConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};

use crate::backends::kafka::config::KafkaConfig;
use crate::types::Topic;

use super::envelope::Envelope;
use super::stage::StageError;

/// A Kafka consumer source that produces a Stream of Result<Envelope, StageError>.
///
/// Wraps rdkafka's StreamConsumer and handles the conversion from
/// BorrowedMessage to Envelope. Users get a clean stream ready to
/// chain .apply_stage() on.
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

    /// Returns a Stream of Result<Envelope, StageError>.
    ///
    /// Consumer errors (broker disconnect, etc.) are logged and dropped —
    /// they're not DLQ candidates since no message was received.
    pub fn stream(
        &self,
    ) -> impl Stream<Item = Result<Envelope<crate::backends::kafka::types::KafkaPayload>, StageError>>
           + '_ {
        self.consumer
            .stream()
            .filter_map(|result| {
                futures::future::ready(match result {
                    Ok(msg) => Some(Ok(Envelope::from_kafka(&msg))),
                    Err(e) => {
                        tracing::error!("Kafka consumer error: {}", e);
                        None
                    }
                })
            })
    }
}
