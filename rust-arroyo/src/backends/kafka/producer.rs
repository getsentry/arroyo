use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer as ArroyoProducer;
use crate::backends::ProducerError;
use crate::timer;
use crate::types::TopicOrPartition;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::producer::{
    DeliveryResult, ProducerContext as RdkafkaProducerContext, ThreadedProducer,
};
use rdkafka::Statistics;
use std::sync::mpsc::{channel, Sender};
use std::time::Duration;

pub struct ProducerContext;

impl ClientContext for ProducerContext {
    fn stats(&self, stats: Statistics) {
        for (broker_id, broker_stats) in &stats.brokers {
            if let Some(int_latency) = &broker_stats.int_latency {
                let p99_latency_ms = int_latency.p99 as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.p99_int_latency",
                    Duration::from_millis(p99_latency_ms as u64),
                    "broker_id" => broker_id.to_string()
                );
                // Also record average latency
                let avg_latency_ms = int_latency.avg as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.avg_int_latency",
                    Duration::from_millis(avg_latency_ms as u64),
                    "broker_id" => broker_id.to_string()
                );
            }
            if let Some(outbuf_latency) = &broker_stats.outbuf_latency {
                let p99_latency_ms = outbuf_latency.p99 as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.p99_outbuf_latency",
                    Duration::from_millis(p99_latency_ms as u64),
                    "broker_id" => broker_id.to_string()
                );
                let avg_latency_ms = outbuf_latency.avg as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.avg_outbuf_latency",
                    Duration::from_millis(avg_latency_ms as u64),
                    "broker_id" => broker_id.to_string()
                );
            }
            if let Some(rtt) = &broker_stats.rtt {
                let p99_rtt_ms = rtt.p99 as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.p99_rtt",
                    Duration::from_millis(p99_rtt_ms as u64),
                    "broker_id" => broker_id.to_string()
                );
                let avg_rtt_ms = rtt.avg as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.avg_rtt",
                    Duration::from_millis(avg_rtt_ms as u64),
                    "broker_id" => broker_id.to_string()
                );
            }
        }
    }
}

impl RdkafkaProducerContext for ProducerContext {
    type DeliveryOpaque = Box<Sender<Option<RDKafkaErrorCode>>>;

    fn delivery(
        &self,
        _delivery_result: &DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        let result = match _delivery_result {
            Ok(_) => Some(RDKafkaErrorCode::NoError),
            Err((err, _)) => err.rdkafka_error_code(),
        };

        let result = _delivery_opaque.send(result);
        result.expect("Failed to send delivery result");
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
        let (tx, rx) = channel::<Option<RDKafkaErrorCode>>();
        let base_record = payload.to_base_record(destination, Box::new(tx));

        // If the producer fails to enqueue the message, this will return an error
        self.producer
            .send(base_record)
            .map_err(|(kafka_error, _record)| ProducerError::from(kafka_error))?;

        // If the producer fails to flush the message out of the buffer, the delivery callback will send an error
        // on the channel.
        // match rx.recv_timeout(Duration::from_secs(5)) {
        match rx.recv() {
            Ok(Some(RDKafkaErrorCode::NoError)) => Ok(()), // The success code
            Ok(Some(err)) => Err(ProducerError::BrokerError { code: err }), // The produce had an error
            Ok(None) => Err(ProducerError::ProducerErrored), // The producer errored with no code
            Err(_) => Err(ProducerError::ProduceWaitTimeout), // The producer timed out waiting for the message to be delivered
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{KafkaProducer, ProducerContext};
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::backends::{Producer, ProducerError};
    use crate::types::{Partition, Topic, TopicOrPartition};
    use rdkafka::client::ClientContext;
    use rdkafka::error::RDKafkaErrorCode;
    use rdkafka::statistics::{Broker, Statistics, Window};
    use std::collections::HashMap;

    fn create_test_statistics_with_all_metrics() -> Statistics {
        let mut brokers = HashMap::new();
        brokers.insert(
            "1".to_string(),
            Broker {
                int_latency: Some(Window {
                    p99: 2000, // microseconds -> 2.0 ms
                    avg: 1000, // microseconds -> 1.0 ms
                    ..Default::default()
                }),
                outbuf_latency: Some(Window {
                    p99: 4000, // microseconds -> 4.0 ms
                    avg: 2000, // microseconds -> 2.0 ms
                    ..Default::default()
                }),
                rtt: Some(Window {
                    p99: 1500, // microseconds -> 1.5 ms
                    avg: 750,  // microseconds -> 0.75 ms
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        Statistics {
            brokers,
            ..Default::default()
        }
    }

    fn create_test_statistics_with_partial_metrics() -> Statistics {
        let mut brokers = HashMap::new();
        brokers.insert(
            "1".to_string(),
            Broker {
                int_latency: Some(Window {
                    p99: 2000,
                    avg: 1000,
                    ..Default::default()
                }),
                outbuf_latency: Some(Window {
                    p99: 4000,
                    avg: 2000,
                    ..Default::default()
                }),
                // No RTT data
                ..Default::default()
            },
        );

        Statistics {
            brokers,
            ..Default::default()
        }
    }

    fn create_test_statistics_empty_brokers() -> Statistics {
        Statistics {
            brokers: HashMap::new(),
            ..Default::default()
        }
    }

    fn create_test_statistics_empty_broker_stats() -> Statistics {
        let mut brokers = HashMap::new();
        brokers.insert("1".to_string(), Broker::default());

        Statistics {
            brokers,
            ..Default::default()
        }
    }

    #[test]
    fn test_producer_context_stats_with_all_metrics() {
        let context = ProducerContext;
        let stats = create_test_statistics_with_all_metrics();

        // This test verifies that the stats callback processes all metrics correctly
        // We can't easily mock the timer! macro, but we can verify the method runs without panicking
        context.stats(stats);
    }

    #[test]
    fn test_producer_context_stats_with_partial_metrics() {
        let context = ProducerContext;
        let stats = create_test_statistics_with_partial_metrics();

        // This test verifies that the stats callback handles missing RTT data gracefully
        context.stats(stats);
    }

    #[test]
    fn test_producer_context_stats_no_brokers() {
        let context = ProducerContext;
        let stats = create_test_statistics_empty_brokers();

        // This test verifies that the stats callback handles empty broker data gracefully
        context.stats(stats);
    }

    #[test]
    fn test_producer_context_stats_empty_broker_stats() {
        let context = ProducerContext;
        let stats = create_test_statistics_empty_broker_stats();

        // This test verifies that the stats callback handles broker with no metrics gracefully
        context.stats(stats);
    }

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
            .expect("Message produced");
    }

    #[test]
    fn test_producer_failure() {
        let topic = Topic::new("doesnotexist"); // This topic does not exist
        let destination = TopicOrPartition::Partition(Partition::new(topic, 1123)); // This partition does not exist
        let configuration =
            KafkaConfig::new_producer_config(vec!["1.0.0.127:2909".to_string()], None); // This broker does not exist

        let producer = KafkaProducer::new(configuration);

        let payload = KafkaPayload::new(None, None, Some("asdf".as_bytes().to_vec()));
        let result = producer.produce(&destination, payload);

        // The producer should fail due to invalid broker/topic/partition
        assert!(
            result.is_err(),
            "Producer should fail with invalid configuration"
        );
        match result.unwrap_err() {
            ProducerError::BrokerError { code } => {
                assert_eq!(code, RDKafkaErrorCode::BrokerNotAvailable)
            }
            other => panic!("Expected BrokerError not {:?}", other),
        }
    }
}
