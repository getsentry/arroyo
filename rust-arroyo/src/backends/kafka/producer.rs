use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::errors::get_error_name;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::ProducerError;
use crate::backends::{
    AsyncProducer as ArroyoAsyncProducer, Producer as ArroyoProducer, ProducerFuture,
};
use crate::types::{Topic, TopicOrPartition};
use rdkafka::client::{Client, ClientContext};
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{
    DeliveryResult, FutureProducer, Producer as _, ProducerContext as RdkafkaProducerContext,
    ThreadedProducer,
};
use rdkafka::{Message, Statistics};
use std::time::Duration;

mod statistics;

pub struct ProducerContext {
    producer_name: String,
}

impl ProducerContext {
    pub fn new(producer_name: String) -> Self {
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
        delivery_result: &DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        let producer_name = self.get_producer_name().to_owned();
        let counter = match delivery_result {
            Ok(message) => metrics::counter!(
                "arroyo.producer.produce_status",
                "status" => "success",
                "topic" => message.topic().to_owned(),
                "producer_name" => producer_name
            ),
            Err((err, message)) => metrics::counter!(
                "arroyo.producer.produce_status",
                "status" => "error",
                "code" => get_error_name(err),
                "topic" => message.topic().to_owned(),
                "producer_name" => producer_name
            ),
        };
        counter.increment(1);
    }
}

pub struct KafkaProducer<C = ProducerContext>
where
    C: RdkafkaProducerContext<DeliveryOpaque = ()> + 'static,
{
    producer: ThreadedProducer<C>,
}

impl KafkaProducer {
    /// Creates a producer, validating topic metadata if configured with
    /// [`KafkaConfig::with_topic_validation`].
    pub fn new(config: KafkaConfig) -> Result<Self, KafkaError> {
        // Extract client.id from config for metrics, default to "unknown"
        let producer_name = config
            .get_config_value("client.id")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let context = ProducerContext::new(producer_name);
        Self::new_with_context(config, context)
    }
}

impl<C> KafkaProducer<C>
where
    C: RdkafkaProducerContext<DeliveryOpaque = ()> + 'static,
{
    /// Creates a producer with a custom context, replacing Arroyo's metrics callbacks.
    ///
    /// Delivery callbacks run once per accepted message. Immediate enqueue errors
    /// return without a callback; producing does not wait for delivery.
    pub fn new_with_context(config: KafkaConfig, context: C) -> Result<Self, KafkaError> {
        let topic_validation = config.topic_validation;
        let config_obj: ClientConfig = config.into();
        let threaded_producer: ThreadedProducer<_> = config_obj.create_with_context(context)?;

        if let Some((topic, timeout)) = topic_validation {
            validate_topic_metadata(threaded_producer.client(), topic, timeout)?;
        }

        Ok(Self {
            producer: threaded_producer,
        })
    }

    /// Validates a physical topic using this producer's existing client.
    ///
    /// This can be called for each topic when sharing a producer across topics.
    /// Blocks while fetching metadata up to `timeout`, and returns an error if
    /// the fetch fails or Kafka reports a topic error.
    pub fn validate_topic(&self, topic: Topic, timeout: Duration) -> Result<(), KafkaError> {
        validate_topic_metadata(self.producer.client(), topic, timeout)
    }

    pub fn context(&self) -> &C {
        self.producer.context().as_ref()
    }

    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }
}

impl<C> ArroyoProducer<KafkaPayload> for KafkaProducer<C>
where
    C: RdkafkaProducerContext<DeliveryOpaque = ()> + 'static,
{
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
    /// Creates a producer, synchronously validating topic metadata if configured
    /// with [`KafkaConfig::with_topic_validation`].
    pub fn new(config: KafkaConfig) -> Result<Self, KafkaError> {
        // Extract client.id from config for metrics, default to "unknown"
        let producer_name = config
            .get_config_value("client.id")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let context = ProducerContext::new(producer_name.clone());
        let topic_validation = config.topic_validation;
        let config_obj: ClientConfig = config.into();
        let future_producer: FutureProducer<_> = config_obj.create_with_context(context)?;

        if let Some((topic, timeout)) = topic_validation {
            validate_topic_metadata(future_producer.client(), topic, timeout)?;
        }

        Ok(Self {
            producer: future_producer,
            producer_name,
        })
    }

    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }
}

fn validate_topic_metadata<C: ClientContext>(
    client: &Client<C>,
    topic: Topic,
    timeout: Duration,
) -> Result<(), KafkaError> {
    let metadata = client.fetch_metadata(Some(topic.as_str()), timeout)?;
    let topic_metadata = metadata
        .topics()
        .iter()
        .find(|metadata| metadata.name() == topic.as_str())
        .ok_or(KafkaError::MetadataFetch(
            RDKafkaErrorCode::UnknownTopicOrPartition,
        ))?;

    if let Some(error) = topic_metadata.error() {
        return Err(KafkaError::MetadataFetch(error.into()));
    }

    Ok(())
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
    use super::{AsyncKafkaProducer, KafkaProducer, RdkafkaProducerContext};
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::backends::{AsyncProducer, Producer, ProducerError};
    use crate::types::{Topic, TopicOrPartition};
    use rdkafka::client::ClientContext;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};
    use rdkafka::message::Message;
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{DeliveryResult, Producer as _};
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
    use std::collections::HashMap;
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::time::Duration;

    fn assert_producer_creation(config: KafkaConfig, expected: Result<(), KafkaError>) {
        assert_eq!(KafkaProducer::new(config.clone()).map(|_| ()), expected);
        let (sender, _) = mpsc::channel();
        assert_eq!(
            KafkaProducer::new_with_context(config.clone(), CapturingContext(sender)).map(|_| ()),
            expected
        );
        assert_eq!(AsyncKafkaProducer::new(config).map(|_| ()), expected);
    }

    #[test]
    fn test_topic_validation_existing_physical_topic() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("physical-topic", 1, 1).unwrap();
        let config = KafkaConfig::new_producer_config(vec![cluster.bootstrap_servers()], None)
            .with_topic_validation(Topic::new("physical-topic"), Duration::from_secs(5));

        assert_producer_creation(config, Ok(()));
    }

    #[test]
    fn test_topic_validation_unknown_topic() {
        let cluster = MockCluster::new(1).unwrap();
        let config = KafkaConfig::new_producer_config(
            vec![cluster.bootstrap_servers()],
            Some(HashMap::from([(
                "allow.auto.create.topics".to_string(),
                "false".to_string(),
            )])),
        )
        .with_topic_validation(Topic::new("unknown-topic"), Duration::from_secs(5));

        assert_producer_creation(
            config,
            Err(KafkaError::MetadataFetch(
                RDKafkaErrorCode::UnknownTopicOrPartition,
            )),
        );
    }

    #[test]
    fn test_topic_validation_metadata_fetch_failure() {
        let config = KafkaConfig::new_producer_config(Vec::new(), None);
        assert_producer_creation(config.clone(), Ok(()));
        let config =
            config.with_topic_validation(Topic::new("physical-topic"), Duration::from_millis(100));

        assert_producer_creation(
            config,
            Err(KafkaError::MetadataFetch(
                RDKafkaErrorCode::BrokerTransportFailure,
            )),
        );
    }

    #[test]
    fn test_topic_validation_reused_producer() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("first-topic", 1, 1).unwrap();
        let timeout = Duration::from_secs(5);
        let config = KafkaConfig::new_producer_config(
            vec![cluster.bootstrap_servers()],
            Some(HashMap::from([(
                "allow.auto.create.topics".to_string(),
                "false".to_string(),
            )])),
        )
        .with_topic_validation(Topic::new("first-topic"), timeout);
        let (producer, _reports) = callback_producer(config);

        assert_eq!(
            producer.validate_topic(Topic::new("first-topic"), timeout),
            Ok(())
        );
        assert_eq!(
            producer.validate_topic(Topic::new("unknown-topic"), timeout),
            Err(KafkaError::MetadataFetch(
                RDKafkaErrorCode::UnknownTopicOrPartition,
            ))
        );
    }

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

    type DeliveryReport = (Option<KafkaError>, String, usize);

    struct CapturingContext(Sender<DeliveryReport>);

    impl ClientContext for CapturingContext {}

    impl RdkafkaProducerContext for CapturingContext {
        type DeliveryOpaque = ();

        fn delivery(&self, result: &DeliveryResult<'_>, (): ()) {
            let (error, message) = match result {
                Ok(message) => (None, message),
                Err((error, message)) => (Some(error.clone()), message),
            };
            let _ = self
                .0
                .send((error, message.topic().to_owned(), message.payload_len()));
        }
    }

    fn callback_producer(
        config: KafkaConfig,
    ) -> (KafkaProducer<CapturingContext>, Receiver<DeliveryReport>) {
        let (sender, reports) = mpsc::channel();
        let producer = KafkaProducer::new_with_context(config, CapturingContext(sender)).unwrap();
        (producer, reports)
    }

    #[test]
    fn test_delivery_callback_success() {
        let cluster = MockCluster::new(1).unwrap();
        let config = KafkaConfig::new_producer_config(vec![cluster.bootstrap_servers()], None);
        let (producer, reports) = callback_producer(config);
        let destination = TopicOrPartition::Topic(Topic::new("test"));

        producer
            .produce(
                &destination,
                KafkaPayload::new(None, None, Some(b"payload".to_vec())),
            )
            .unwrap();
        producer.producer.flush(Duration::from_secs(10)).unwrap();
        drop(producer);
        assert_eq!(
            reports.into_iter().collect::<Vec<_>>(),
            vec![(None, "test".to_owned(), 7)]
        );
    }

    #[test]
    fn test_delivery_callback_broker_error() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.request_errors(
            RDKafkaApiKey::Produce,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE],
        );
        let config = KafkaConfig::new_producer_config(vec![cluster.bootstrap_servers()], None);
        let (producer, reports) = callback_producer(config);
        let destination = TopicOrPartition::Topic(Topic::new("test"));

        producer
            .produce(
                &destination,
                KafkaPayload::new(None, None, Some(b"payload".to_vec())),
            )
            .unwrap();
        producer.producer.flush(Duration::from_secs(10)).unwrap();
        drop(producer);
        assert_eq!(
            reports.into_iter().collect::<Vec<_>>(),
            vec![(
                Some(KafkaError::MessageProduction(
                    RDKafkaErrorCode::MessageSizeTooLarge
                )),
                "test".to_owned(),
                7,
            )]
        );
    }

    #[test]
    fn test_delivery_callback_only_for_accepted_messages() {
        let (producer, reports) = callback_producer(queue_full_configuration());
        let destination = TopicOrPartition::Topic(Topic::new("test"));
        producer
            .produce(
                &destination,
                KafkaPayload::new(None, None, Some(b"accepted".to_vec())),
            )
            .unwrap();
        assert!(producer
            .produce(
                &destination,
                KafkaPayload::new(None, None, Some(b"no".to_vec()))
            )
            .is_err());

        producer.producer.flush(Duration::from_secs(10)).unwrap();
        drop(producer);
        assert_eq!(
            reports.into_iter().collect::<Vec<_>>(),
            vec![(
                Some(KafkaError::MessageProduction(
                    RDKafkaErrorCode::MessageTimedOut
                )),
                "test".to_owned(),
                8,
            )]
        );
    }
}
