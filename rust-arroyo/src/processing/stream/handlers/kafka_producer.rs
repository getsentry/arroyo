use std::sync::Arc;

use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer;
use crate::types::TopicOrPartition;

use super::super::pipeline_envelope::PipelineEnvelope;
use super::next::NextHandler;

/// A canned SuccessHandler that produces the envelope's payload to a Kafka topic.
pub struct KafkaProducerHandler {
    producer: Arc<dyn Producer<KafkaPayload>>,
    topic: TopicOrPartition,
}

impl KafkaProducerHandler {
    pub fn new(producer: impl Producer<KafkaPayload> + 'static, topic: TopicOrPartition) -> Self {
        Self {
            producer: Arc::new(producer),
            topic,
        }
    }
}

impl NextHandler<KafkaPayload> for KafkaProducerHandler {
    async fn handle(
        &self,
        envelope: &PipelineEnvelope<KafkaPayload>,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.producer
            .produce(&self.topic, envelope.payload.clone())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)
    }
}
