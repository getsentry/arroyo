use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::errors::get_error_name;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::ProducerError;
use crate::backends::{
    AsyncProducer as ArroyoAsyncProducer, Producer as ArroyoProducer, ProducerFuture,
};
use crate::types::TopicOrPartition;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{
    DeliveryResult, FutureProducer, Producer as _, ProducerContext as RdkafkaProducerContext,
    ThreadedProducer,
};
use rdkafka::Statistics;

mod statistics;

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
        statistics::record(&stats, self.get_producer_name());
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
        metrics::counter!(
            "arroyo.producer.produce_status",
            "status" => result,
            "producer_name" => producer_name.to_owned()
        )
        .increment(1);
    }
}

pub struct KafkaProducer {
    producer: ThreadedProducer<ProducerContext>,
}

impl KafkaProducer {
    pub fn new(config: KafkaConfig) -> Result<Self, KafkaError> {
        // Extract client.id from config for metrics, default to "unknown"
        let producer_name = config
            .get_config_value("client.id")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let context = ProducerContext::new(producer_name.clone());
        let config_obj: ClientConfig = config.into();
        let threaded_producer: ThreadedProducer<_> = config_obj.create_with_context(context)?;

        Ok(Self {
            producer: threaded_producer,
        })
    }

    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
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
    pub fn new(config: KafkaConfig) -> Result<Self, KafkaError> {
        // Extract client.id from config for metrics, default to "unknown"
        let producer_name = config
            .get_config_value("client.id")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let context = ProducerContext::new(producer_name.clone());
        let config_obj: ClientConfig = config.into();
        let future_producer: FutureProducer<_> = config_obj.create_with_context(context)?;

        Ok(Self {
            producer: future_producer,
            producer_name,
        })
    }

    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
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
        metrics::counter!(
            "arroyo.producer.produce_status",
            "status" => "error",
            "code" => error_name,
            "producer_name" => producer_name.to_owned()
        )
        .increment(1);
        return producer_error;
    }
    let producer_error = ProducerError::ProducerFailure {
        error: default_error.to_string(),
    };
    metrics::counter!(
        "arroyo.producer.produce_status",
        "status" => "error",
        "code" => default_error.to_owned(),
        "producer_name" => producer_name.to_owned()
    )
    .increment(1);
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
    use super::{AsyncKafkaProducer, KafkaProducer};
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::backends::{AsyncProducer, Producer, ProducerError};
    use crate::types::{Topic, TopicOrPartition};
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};
    use std::collections::HashMap;

    fn queue_full_configuration() -> KafkaConfig {
        KafkaConfig::new_producer_config(
            Vec::new(),
            Some(HashMap::from([
                ("queue.buffering.max.messages".to_string(), "1".to_string()),
                ("message.timeout.ms".to_string(), "5000".to_string()),
            ])),
        )
    }

    #[test]
    fn test_producer() {
        let topic = Topic::new("test");
        let destination = TopicOrPartition::Topic(topic);
        let configuration =
            KafkaConfig::new_producer_config(vec!["127.0.0.1:9092".to_string()], None);

        let producer = KafkaProducer::new(configuration);
        assert!(producer.is_ok());
        let producer = producer.unwrap();

        let payload = KafkaPayload::new(None, None, Some("asdf".as_bytes().to_vec()));
        producer
            .produce(&destination, payload)
            .expect("Message produced");
    }

    #[tokio::test]
    async fn test_async_producer() {
        let topic = Topic::new("test");
        let destination = TopicOrPartition::Topic(topic);
        let configuration =
            KafkaConfig::new_producer_config(vec!["127.0.0.1:9092".to_string()], None);

        let producer = AsyncKafkaProducer::new(configuration);
        assert!(producer.is_ok());
        let producer = producer.unwrap();

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
        assert!(producer.is_ok());
        let producer = producer.unwrap();

        let payload = KafkaPayload::new(None, None, Some("asdf".as_bytes().to_vec()));
        let result = producer.produce(&destination, payload).await;
        assert!(
            result.is_err(),
            "Message should not be produced successfully"
        );
    }

    #[test]
    fn test_sync_enqueue_error_retains_queue_full_error() {
        let producer = KafkaProducer::new(queue_full_configuration());
        assert!(producer.is_ok());
        let producer = producer.unwrap();
        let destination = TopicOrPartition::Topic(Topic::new("test"));

        let first_result = producer.produce(
            &destination,
            KafkaPayload::new(None, None, Some(b"first".to_vec())),
        );
        assert!(first_result.is_ok());

        let second_result = producer.produce(
            &destination,
            KafkaPayload::new(None, None, Some(b"second".to_vec())),
        );
        assert!(matches!(
            second_result,
            Err(ProducerError::Kafka(KafkaError::MessageProduction(
                RDKafkaErrorCode::QueueFull
            )))
        ));
    }

    #[test]
    fn test_invalid_producer_configuration_returns_error() {
        let configuration = KafkaConfig::new_producer_config(
            vec!["127.0.0.1:9092".to_string()],
            Some(HashMap::from([(
                "message.timeout.ms".to_string(),
                "invalid".to_string(),
            )])),
        );

        assert!(matches!(
            KafkaProducer::new(configuration),
            Err(KafkaError::ClientConfig(..))
        ));
    }

    #[test]
    fn test_invalid_async_producer_configuration_returns_error() {
        let configuration = KafkaConfig::new_producer_config(
            vec!["127.0.0.1:9092".to_string()],
            Some(HashMap::from([(
                "message.timeout.ms".to_string(),
                "invalid".to_string(),
            )])),
        );

        assert!(matches!(
            AsyncKafkaProducer::new(configuration),
            Err(KafkaError::ClientConfig(..))
        ));
    }
}
