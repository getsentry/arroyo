use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::errors::get_error_name;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::ProducerError;
use crate::backends::{
    AsyncProducer as ArroyoAsyncProducer, Producer as ArroyoProducer, ProducerFuture,
};
use crate::counter;
use crate::gauge;
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

pub struct ProducerContext {
    producer_name: String,
}

impl ProducerContext {
    fn new(producer_name: String) -> Self {
        Self { producer_name }
    }

    fn get_producer_name(&self) -> &str {
        &self.producer_name
    }
}

impl ClientContext for ProducerContext {
    fn stats(&self, stats: Statistics) {
        let producer_name = self.get_producer_name();

        for (broker_id, broker_stats) in &stats.brokers {
            let broker_id_str = broker_id.to_string();

            // Record broker latency metrics
            if let Some(int_latency) = &broker_stats.int_latency {
                let p99_latency_ms = int_latency.p99 as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.p99_int_latency",
                    Duration::from_millis(p99_latency_ms as u64),
                    "broker_id" => broker_id_str.clone(),
                    "producer_name" => producer_name
                );

                let avg_latency_ms = int_latency.avg as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.avg_int_latency",
                    Duration::from_millis(avg_latency_ms as u64),
                    "broker_id" => broker_id_str.clone(),
                    "producer_name" => producer_name
                );
            }

            if let Some(outbuf_latency) = &broker_stats.outbuf_latency {
                let p99_latency_ms = outbuf_latency.p99 as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.p99_outbuf_latency",
                    Duration::from_millis(p99_latency_ms as u64),
                    "broker_id" => broker_id_str.clone(),
                    "producer_name" => producer_name
                );

                let avg_latency_ms = outbuf_latency.avg as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.avg_outbuf_latency",
                    Duration::from_millis(avg_latency_ms as u64),
                    "broker_id" => broker_id_str.clone(),
                    "producer_name" => producer_name
                );
            }

            if let Some(rtt) = &broker_stats.rtt {
                let p99_rtt_ms = rtt.p99 as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.p99_rtt",
                    Duration::from_millis(p99_rtt_ms as u64),
                    "broker_id" => broker_id_str.clone(),
                    "producer_name" => producer_name
                );

                let avg_rtt_ms = rtt.avg as f64 / 1000.0;
                timer!(
                    "arroyo.producer.librdkafka.avg_rtt",
                    Duration::from_millis(avg_rtt_ms as u64),
                    "broker_id" => broker_id_str.clone(),
                    "producer_name" => producer_name
                );
            }

            // Record broker transmission metrics
            gauge!(
                "arroyo.producer.librdkafka.broker_tx",
                broker_stats.tx as i64,
                "broker_id" => broker_id_str.clone(),
                "producer_name" => producer_name
            );

            gauge!(
                "arroyo.producer.librdkafka.broker_txbytes",
                broker_stats.txbytes as i64,
                "broker_id" => broker_id_str.clone(),
                "producer_name" => producer_name
            );

            // Record broker buffer metrics
            gauge!(
                "arroyo.producer.librdkafka.broker_outbuf_requests",
                broker_stats.outbuf_cnt as i64,
                "broker_id" => broker_id_str.clone(),
                "producer_name" => producer_name
            );

            gauge!(
                "arroyo.producer.librdkafka.broker_outbuf_messages",
                broker_stats.outbuf_msg_cnt as i64,
                "broker_id" => broker_id_str.clone(),
                "producer_name" => producer_name
            );

            // Record broker connection metrics (if available)
            if let Some(connects) = broker_stats.connects {
                gauge!(
                    "arroyo.producer.librdkafka.broker_connects",
                    connects as i64,
                    "broker_id" => broker_id_str.clone(),
                    "producer_name" => producer_name
                );
            }

            if let Some(disconnects) = broker_stats.disconnects {
                gauge!(
                    "arroyo.producer.librdkafka.broker_disconnects",
                    disconnects as i64,
                    "broker_id" => broker_id_str.clone(),
                    "producer_name" => producer_name
                );
            }

            // Record broker transmission error metrics
            gauge!(
                "arroyo.producer.librdkafka.broker_txerrs",
                broker_stats.txerrs as i64,
                "broker_id" => broker_id_str.clone(),
                "producer_name" => producer_name
            );

            gauge!(
                "arroyo.producer.librdkafka.broker_txretries",
                broker_stats.txretries as i64,
                "broker_id" => broker_id_str,
                "producer_name" => producer_name
            );
        }

        // Record global producer metrics
        gauge!(
            "arroyo.producer.librdkafka.message_count",
            stats.msg_cnt as i64,
            "producer_name" => producer_name
        );

        gauge!(
            "arroyo.producer.librdkafka.message_count_max",
            stats.msg_max as i64,
            "producer_name" => producer_name
        );

        gauge!(
            "arroyo.producer.librdkafka.message_size",
            stats.msg_size as i64,
            "producer_name" => producer_name
        );

        gauge!(
            "arroyo.producer.librdkafka.message_size_max",
            stats.msg_size_max as i64,
            "producer_name" => producer_name
        );

        gauge!(
            "arroyo.producer.librdkafka.txmsgs",
            stats.txmsgs as i64,
            "producer_name" => producer_name
        );
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
        let producer_name = self.get_producer_name();
        counter!("arroyo.producer.produce_status", 1, "status" => result, "producer_name" => producer_name);
    }
}

pub struct KafkaProducer {
    producer: ThreadedProducer<ProducerContext>,
}

impl KafkaProducer {
    pub fn new(config: KafkaConfig) -> Self {
        // Extract client.id from config for metrics, default to "unknown"
        let producer_name = config
            .get_config_value("client.id")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let context = ProducerContext::new(producer_name.clone());
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
    producer_name: String,
}

impl AsyncKafkaProducer {
    pub fn new(config: KafkaConfig) -> Self {
        // Extract client.id from config for metrics, default to "unknown"
        let producer_name = config
            .get_config_value("client.id")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let context = ProducerContext::new(producer_name.clone());
        let config_obj: ClientConfig = config.into();
        let future_producer: FutureProducer<_> = config_obj.create_with_context(context).unwrap();

        Self {
            producer: future_producer,
            producer_name,
        }
    }
}

fn record_producer_error(
    kafka_error: Option<KafkaError>,
    default_error: &str,
    producer_name: &str,
) -> ProducerError {
    if let Some(kafka_error) = kafka_error {
        let error_name = get_error_name(&kafka_error);
        let producer_error = ProducerError::ProducerFailure {
            error: error_name.clone(),
        };
        counter!("arroyo.producer.produce_status", 1, "status" => "error", "code" => error_name, "producer_name" => producer_name);
        return producer_error;
    }
    let producer_error = ProducerError::ProducerFailure {
        error: default_error.to_string(),
    };
    counter!("arroyo.producer.produce_status", 1, "status" => "error", "code" => default_error, "producer_name" => producer_name);
    producer_error
}

impl ArroyoAsyncProducer<KafkaPayload> for AsyncKafkaProducer {
    fn produce(&self, destination: &TopicOrPartition, payload: KafkaPayload) -> ProducerFuture {
        let base_record = payload.to_future_record(destination);

        let producer_name = self.producer_name.clone();
        let queue_result = self.producer.send_result(base_record);
        if queue_result.is_err() {
            // If the producer couldn't put the message in the queue at all, it won't retry and will return an error directly
            let producer_error = record_producer_error(
                queue_result.err().map(|(kafka_error, _record)| kafka_error),
                "queue_full",
                &producer_name,
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
                        let producer_error = record_producer_error(
                            Some(kafka_error),
                            "produce_error",
                            &producer_name,
                        );
                        Err(producer_error)
                    }
                },
                Err(_canceled) => {
                    // The future was canceled, which means the producer was closed
                    let producer_error =
                        record_producer_error(None, "future_canceled", &producer_name);
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
        let context = ProducerContext::new("unknown".to_string());
        let stats = create_test_statistics_with_all_metrics();

        // This test verifies that the stats callback processes all metrics correctly
        // We can't easily mock the timer! macro, but we can verify the method runs without panicking
        context.stats(stats);
    }

    #[test]
    fn test_producer_context_stats_with_partial_metrics() {
        let context = ProducerContext::new("unknown".to_string());
        let stats = create_test_statistics_with_partial_metrics();

        // This test verifies that the stats callback handles missing RTT data gracefully
        context.stats(stats);
    }

    #[test]
    fn test_producer_context_stats_no_brokers() {
        let context = ProducerContext::new("unknown".to_string());
        let stats = create_test_statistics_empty_brokers();

        // This test verifies that the stats callback handles empty broker data gracefully
        context.stats(stats);
    }

    #[test]
    fn test_producer_context_stats_empty_broker_stats() {
        let context = ProducerContext::new("unknown".to_string());
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
