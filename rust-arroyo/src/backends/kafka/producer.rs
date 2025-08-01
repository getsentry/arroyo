use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::errors::get_error_name;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::ProducerError;
use crate::backends::{
    AsyncProducer as ArroyoAsyncProducer, Producer as ArroyoProducer, ProducerFuture,
};
use crate::counter;
use crate::timer;
use crate::types::TopicOrPartition;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{
    DeliveryResult, FutureProducer, ProducerContext as RdkafkaProducerContext, ThreadedProducer,
};
use rdkafka::Statistics;
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
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        _delivery_result: &DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        let result = match _delivery_result {
            Ok(_) => "success".to_string(),
            Err((err, _)) => get_error_name(err),
        };
        counter!("arroyo.producer.produce_status", 1, "status" => result);
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
            .map_err(|(kafka_error, _record)| ProducerError::from(kafka_error))?;

        Ok(())
    }
}

pub struct AsyncKafkaProducer {
    producer: FutureProducer<ProducerContext>,
}

impl AsyncKafkaProducer {
    pub fn new(config: KafkaConfig) -> Self {
        let context = ProducerContext;
        let config_obj: ClientConfig = config.into();
        let future_producer: FutureProducer<_> = config_obj.create_with_context(context).unwrap();

        Self {
            producer: future_producer,
        }
    }
}

fn record_producer_error(kafka_error: Option<KafkaError>, default_error: &str) -> ProducerError {
    if let Some(kafka_error) = kafka_error {
        let error_name = get_error_name(&kafka_error);
        let producer_error = ProducerError::ProducerFailure {
            error: error_name.clone(),
        };
        counter!("arroyo.producer.produce_status", 1, "status" => "error", "code" => error_name);
        return producer_error;
    }
    let producer_error = ProducerError::ProducerFailure {
        error: default_error.to_string(),
    };
    counter!("arroyo.producer.produce_status", 1, "status" => "error", "code" => default_error);
    producer_error
}

impl ArroyoAsyncProducer<KafkaPayload> for AsyncKafkaProducer {
    fn produce(&self, destination: &TopicOrPartition, payload: KafkaPayload) -> ProducerFuture {
        let base_record = payload.to_future_record(destination);

        let queue_result = self.producer.send_result(base_record);
        if queue_result.is_err() {
            // If the producer couldn't put the message in the queue at all, it won't retry and will return an error directly
            let producer_error = record_producer_error(
                queue_result.err().map(|(kafka_error, _record)| kafka_error),
                "queue_full",
            );
            return Box::pin(async move { Err(producer_error) });
        }

        let future = queue_result.unwrap();

        Box::pin(async move {
            let produce_result = match future.await {
                Ok(delivery_result) => match delivery_result {
                    Ok(_) => Ok(()),
                    Err((kafka_error, _record)) => {
                        // The producer failed when flushing the message out of the queue
                        let producer_error =
                            record_producer_error(Some(kafka_error), "produce_error");
                        Err(producer_error)
                    }
                },
                Err(_canceled) => {
                    // The future was canceled, which means the producer was closed
                    let producer_error = record_producer_error(None, "future_canceled");
                    Err(producer_error)
                }
            };

            produce_result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{AsyncKafkaProducer, KafkaProducer, ProducerContext};
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::backends::{AsyncProducer, Producer};
    use crate::types::{Topic, TopicOrPartition};
    use rdkafka::client::ClientContext;
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
            .expect("Message produced")
    }

    #[tokio::test]
    async fn test_async_producer() {
        let topic = Topic::new("test");
        let destination = TopicOrPartition::Topic(topic);
        let configuration =
            KafkaConfig::new_producer_config(vec!["127.0.0.1:9092".to_string()], None);

        let producer = AsyncKafkaProducer::new(configuration);

        let payload = KafkaPayload::new(None, None, Some("asdf".as_bytes().to_vec()));
        let result = producer.produce(&destination, payload).await;
        assert!(result.is_ok(), "Message should be produced successfully");
    }

    #[tokio::test]
    async fn test_async_producer_with_error() {
        let topic = Topic::new("test");
        let destination = TopicOrPartition::Topic(topic);
        let configuration = KafkaConfig::new_producer_config(
            vec!["obviously-not-a-valid-broker".to_string()],
            Some(HashMap::from([(
                "message.timeout.ms".to_string(),
                "1".to_string(),
            )])),
        );

        let producer = AsyncKafkaProducer::new(configuration);

        let payload = KafkaPayload::new(None, None, Some("asdf".as_bytes().to_vec()));
        let result = producer.produce(&destination, payload).await;
        assert!(
            result.is_err(),
            "Message should not be produced successfully"
        );
    }
}
