use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer as ArroyoProducer;
use crate::backends::ProducerError;
use crate::timer;
use crate::types::TopicOrPartition;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{
    DeliveryResult, ProducerContext as RdkafkaProducerContext, ThreadedProducer,
};
use rdkafka::Statistics;
use std::time::Duration;

pub struct ProducerContext;

impl ClientContext for ProducerContext {
    fn stats(&self, stats: Statistics) {
        // Extract broker-level int_latency metrics
        for (broker_id, broker_stats) in &stats.brokers {
            if let Some(int_latency) = &broker_stats.int_latency {
                // Use p99 latency as the primary metric (microseconds -> milliseconds)
                let p99_latency_ms = int_latency.p99 as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.p99_int_latency",
                    Duration::from_millis(p99_latency_ms as u64),
                    "broker_id" => broker_id.to_string()
                );
            }
            if let Some(outbuf_latency) = &broker_stats.outbuf_latency {
                // Use p99 latency as the primary metric (microseconds -> milliseconds)
                let p99_latency_ms = outbuf_latency.p99 as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.p99_outbuf_latency",
                    Duration::from_millis(p99_latency_ms as u64),
                    "broker_id" => broker_id.to_string()
                );
            }
            if let Some(rtt) = &broker_stats.rtt {
                // Use p99 RTT as the primary metric (microseconds -> milliseconds)
                let p99_rtt_ms = rtt.p99 as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.p99_rtt",
                    Duration::from_millis(p99_rtt_ms as u64),
                    "broker_id" => broker_id.to_string()
                );
            }
        }
    }
}

impl RdkafkaProducerContext for ProducerContext {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        _delivery_result: &DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
    }
}

pub struct KafkaProducer {
    producer: ThreadedProducer<ProducerContext>,
}

impl KafkaProducer {
    pub fn new(config: KafkaConfig) -> Self {
        let context = ProducerContext;
        let config_obj: ClientConfig = config.into();
        let threaded_producer: ThreadedProducer<_> =
            config_obj.create_with_context(context).unwrap();

        Self {
            producer: threaded_producer,
        }
    }
}

impl ArroyoProducer<KafkaPayload> for KafkaProducer {
    fn produce(
        &self,
        destination: &TopicOrPartition,
        payload: KafkaPayload,
    ) -> Result<(), ProducerError> {
        let base_record = payload.to_base_record(destination);

        self.producer
            .send(base_record)
            .map_err(|_| ProducerError::ProducerErrorred)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{KafkaProducer, ProducerContext};
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::backends::Producer;
    use crate::types::{Topic, TopicOrPartition};

    #[test]
    fn test_producer() {
        let topic = Topic::new("test");
        let destination = TopicOrPartition::Topic(topic);
        let configuration =
            KafkaConfig::new_producer_config(vec!["127.0.0.1:9092".to_string()], None);

        let producer = KafkaProducer::new(configuration);

        let payload = KafkaPayload::new(None, None, Some("asdf".as_bytes().to_vec()));
        producer
            .produce(&destination, payload)
            .expect("Message produced")
    }
}
