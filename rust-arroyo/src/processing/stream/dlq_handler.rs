use std::sync::Arc;

use crate::backends::kafka::types::{Headers, KafkaPayload};
use crate::backends::Producer;
use crate::types::TopicOrPartition;

use super::error_handler::{ErrorContext, ErrorHandler};

/// A canned ErrorHandler that produces the original message to a DLQ Kafka topic.
/// Adds original_partition and original_offset headers so the message can be
/// traced back to its source.
///
/// This is the pull-model equivalent of KafkaDlqProducer in dlq.rs.
pub struct DlqErrorHandler {
    producer: Arc<dyn Producer<KafkaPayload>>,
    topic: TopicOrPartition,
}

impl DlqErrorHandler {
    pub fn new(producer: impl Producer<KafkaPayload> + 'static, topic: TopicOrPartition) -> Self {
        Self {
            producer: Arc::new(producer),
            topic,
        }
    }
}

impl ErrorHandler for DlqErrorHandler {
    async fn handle(
        &self,
        error: ErrorContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let origin = &error.origin;

        tracing::error!(
            "DLQ: {:?}:{} kind={} error={}",
            origin.partition,
            origin.offset,
            error.kind,
            error.source,
        );

        // Build a new payload with original_partition and original_offset headers,
        // same as KafkaDlqProducer in dlq.rs:76-107.
        let headers = Headers::new()
            .insert(
                "original_partition",
                Some(origin.partition.index.to_string().into_bytes()),
            )
            .insert(
                "original_offset",
                Some(origin.offset.to_string().into_bytes()),
            );

        let payload = KafkaPayload::new(
            origin.payload.key().cloned(),
            Some(headers),
            origin.payload.payload().cloned(),
        );

        self.producer
            .produce(&self.topic, payload)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)
    }
}
