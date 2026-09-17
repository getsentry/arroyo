use std::sync::Arc;

use crate::backends::kafka::types::{Headers, KafkaPayload};
use crate::backends::Producer;
use crate::types::TopicOrPartition;

use crate::processing::stream::BoxError;

use super::rejection::{RejectionHandler, RejectionMetadata};

/// A canned RejectionHandler that produces the original message to a DLQ Kafka topic.
pub struct DlqHandler {
    producer: Arc<dyn Producer<KafkaPayload>>,
    topic: TopicOrPartition,
}

impl DlqHandler {
    pub fn new(producer: Arc<dyn Producer<KafkaPayload>>, topic: TopicOrPartition) -> Self {
        Self { producer, topic }
    }
}

impl RejectionHandler for DlqHandler {
    async fn handle(&self, rejected: &RejectionMetadata) -> Result<(), BoxError> {
        tracing::error!(
            "DLQ: {:?}:{} reason={:?}",
            rejected.metadata.partition,
            rejected.metadata.offset,
            rejected.reason,
        );

        // Preserve original message headers and append partition/offset metadata.
        let headers = rejected
            .raw
            .headers()
            .cloned()
            .unwrap_or_else(Headers::new)
            .insert(
                "original_partition",
                Some(rejected.metadata.partition.index.to_string().into_bytes()),
            )
            .insert(
                "original_offset",
                Some(rejected.metadata.offset.to_string().into_bytes()),
            );

        let payload = KafkaPayload::new(
            rejected.raw.key().cloned(),
            Some(headers),
            rejected.raw.payload().cloned(),
        );

        self.producer
            .produce(&self.topic, payload)
            .map_err(|e| Box::new(e) as BoxError)
    }
}
