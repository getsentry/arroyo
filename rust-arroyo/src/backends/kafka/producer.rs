use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::errors::get_error_name;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::ProducerError;
use crate::backends::{
    AsyncProducer as ArroyoAsyncProducer, Producer as ArroyoProducer, ProducerFuture,
};
use crate::metrics;
use crate::types::{Topic, TopicOrPartition};
use rdkafka::client::{Client, ClientContext, OAuthToken};
use rdkafka::config::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::ToBytes;
use rdkafka::producer::{
    BaseRecord, DeliveryResult, FutureProducer, Producer as _,
    ProducerContext as RdkafkaProducerContext, ThreadedProducer,
};
use rdkafka::{Message, Statistics};
use std::error::Error;
use std::time::{Duration, Instant};

mod statistics;

/// Producer context that does nothing. The default for producers that do not
/// need their own callbacks.
///
/// Arroyo's metrics do not depend on the context: every producer records them,
/// whatever context it is built with.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopProducerContext;

impl ClientContext for NoopProducerContext {
    // Overridden because rdkafka's default logs every statistics payload.
    fn stats(&self, _statistics: Statistics) {}
}

impl RdkafkaProducerContext for NoopProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, _: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {}
}

/// Wraps the caller's context to record Arroyo's metrics before handing each
/// callback on, so a custom context cannot switch the metrics off.
struct MetricsContext<C> {
    producer_name: String,
    inner: C,
}

impl<C: ClientContext> ClientContext for MetricsContext<C> {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = C::ENABLE_REFRESH_OAUTH_TOKEN;

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.inner.log(level, fac, log_message);
    }

    fn stats(&self, statistics: Statistics) {
        statistics::record(&statistics, &self.producer_name);
        self.inner.stats(statistics);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.inner.error(error, reason);
    }

    fn generate_oauth_token(
        &self,
        oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        self.inner.generate_oauth_token(oauthbearer_config)
    }
}

impl<C> RdkafkaProducerContext for MetricsContext<C>
where
    C: RdkafkaProducerContext<DeliveryOpaque = ()>,
{
    type DeliveryOpaque = ();

    fn delivery(&self, delivery_result: &DeliveryResult<'_>, delivery_opaque: ()) {
        let (message, error) = match delivery_result {
            Ok(message) => (message, None),
            Err((error, message)) => (message, Some(get_error_name(error))),
        };
        record_produce_status(&self.producer_name, message.topic(), error);
        self.inner.delivery(delivery_result, delivery_opaque);
    }
}

pub struct KafkaProducer<C = NoopProducerContext>
where
    C: RdkafkaProducerContext<DeliveryOpaque = ()> + 'static,
{
    producer: ThreadedProducer<MetricsContext<C>>,
    producer_name: String,
}

impl KafkaProducer {
    pub fn new(config: KafkaConfig) -> Result<Self, KafkaError> {
        Self::new_with_context(config, NoopProducerContext)
    }
}

impl<C> KafkaProducer<C>
where
    C: RdkafkaProducerContext<DeliveryOpaque = ()> + 'static,
{
    /// Creates a producer with a custom context for the caller's own callbacks.
    ///
    /// Delivery callbacks run once per accepted message. Immediate enqueue errors
    /// return without a callback; producing does not wait for delivery.
    ///
    /// Arroyo's metrics are recorded either way; the context's callbacks run
    /// after them.
    pub fn new_with_context(config: KafkaConfig, context: C) -> Result<Self, KafkaError> {
        let producer_name = producer_name_of(&config);
        let context = MetricsContext {
            producer_name: producer_name.clone(),
            inner: context,
        };
        let config_obj: ClientConfig = config.into();
        Ok(Self {
            producer: config_obj.create_with_context(context)?,
            producer_name,
        })
    }

    pub fn produce_record<K, P>(&self, record: BaseRecord<'_, K, P>) -> Result<(), ProducerError>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let topic = record.topic;
        self.producer
            .send(record)
            .map_err(|(kafka_error, _record)| {
                record_produce_status(
                    &self.producer_name,
                    topic,
                    Some(get_error_name(&kafka_error)),
                );
                ProducerError::from(kafka_error)
            })
    }

    /// Synchronously validates a physical topic
    pub fn validate_topic(&self, topic: Topic, timeout: Duration) -> Result<(), KafkaError> {
        validate_topic_metadata(self.producer.client(), topic, timeout)
    }

    pub fn context(&self) -> &C {
        &self.producer.context().inner
    }

    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }

    /// Blocks until the queue is delivered or the timeout expires. Messages
    /// remain queued on timeout.
    ///
    /// Retries against a wall-clock deadline because rdkafka before 0.38
    /// budgets `flush` by poll count, so early-returning polls can exhaust its
    /// timeout before the requested time passes.
    pub fn flush_for(&self, timeout: Duration) -> Result<(), KafkaError> {
        let deadline = Instant::now() + timeout;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.producer.flush(remaining) {
                Ok(()) => return Ok(()),
                Err(error) if remaining.is_zero() => return Err(error),
                Err(_) => {}
            }
        }
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
        self.produce_record(base_record)
    }
}

pub struct AsyncKafkaProducer {
    producer: FutureProducer<MetricsContext<NoopProducerContext>>,
    producer_name: String,
}

impl AsyncKafkaProducer {
    /// Creates a producer
    pub fn new(config: KafkaConfig) -> Result<Self, KafkaError> {
        let producer_name = producer_name_of(&config);
        let context = MetricsContext {
            producer_name: producer_name.clone(),
            inner: NoopProducerContext,
        };
        let config_obj: ClientConfig = config.into();
        Ok(Self {
            producer: config_obj.create_with_context(context)?,
            producer_name,
        })
    }

    /// Synchronously validates a physical topic
    pub fn validate_topic(&self, topic: Topic, timeout: Duration) -> Result<(), KafkaError> {
        validate_topic_metadata(self.producer.client(), topic, timeout)
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

/// Producer name in metric tags when `client.id` is not set.
pub(super) const UNNAMED_PRODUCER: &str = "unknown";

/// Returns the producer name used in metric tags.
pub(super) fn producer_name_of(config: &KafkaConfig) -> String {
    config
        .get_config_value("client.id")
        .cloned()
        .unwrap_or_else(|| UNNAMED_PRODUCER.to_owned())
}

fn record_produce_status(producer_name: &str, topic: &str, error: Option<String>) {
    let counter = match error {
        Some(error) => metrics::counter!(
            "arroyo.producer.produce_status",
            "status" => "error",
            "code" => error,
            "topic" => topic.to_owned(),
            "producer_name" => producer_name.to_owned()
        ),
        None => metrics::counter!(
            "arroyo.producer.produce_status",
            "status" => "success",
            "topic" => topic.to_owned(),
            "producer_name" => producer_name.to_owned()
        ),
    };
    counter.increment(1);
}

impl ArroyoAsyncProducer<KafkaPayload> for AsyncKafkaProducer {
    fn produce(&self, destination: &TopicOrPartition, payload: KafkaPayload) -> ProducerFuture {
        let record = payload.to_future_record(destination);
        let topic = record.topic.to_owned();
        let producer_name = self.producer_name.clone();
        // Enqueue immediately, without waiting for the returned future to be polled.
        let future = match self.producer.send_result(record) {
            Ok(future) => future,
            Err((error, _record)) => {
                record_produce_status(&producer_name, &topic, Some(get_error_name(&error)));
                return Box::pin(async move { Err(error.into()) });
            }
        };

        Box::pin(async move {
            // FutureProducer completes its own delivery future rather than calling
            // MetricsContext::delivery, so record the outcome here.
            match future.await {
                Ok(Ok(_)) => {
                    record_produce_status(&producer_name, &topic, None);
                    Ok(())
                }
                Ok(Err((error, _record))) => {
                    record_produce_status(&producer_name, &topic, Some(get_error_name(&error)));
                    Err(error.into())
                }
                Err(_canceled) => {
                    record_produce_status(
                        &producer_name,
                        &topic,
                        Some("future_canceled".to_owned()),
                    );
                    Err(ProducerError::ProducerFailure {
                        error: "future_canceled".to_owned(),
                    })
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use metrics::{
        Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };

    use rdkafka::client::ClientContext;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};
    use rdkafka::message::{Header, Message, OwnedHeaders};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{BaseRecord, DeliveryResult, Producer as _};
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};

    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::backends::{AsyncProducer, Producer, ProducerError};
    use crate::types::{Topic, TopicOrPartition};

    use super::{AsyncKafkaProducer, KafkaProducer, RdkafkaProducerContext};

    fn assert_topic_validation(
        config: KafkaConfig,
        topic: Topic,
        timeout: Duration,
        expected: Result<(), KafkaError>,
    ) {
        assert_eq!(
            KafkaProducer::new(config.clone())
                .unwrap()
                .validate_topic(topic, timeout),
            expected
        );
        let (sender, _) = mpsc::channel();
        assert_eq!(
            KafkaProducer::new_with_context(config.clone(), CapturingContext(sender))
                .unwrap()
                .validate_topic(topic, timeout),
            expected
        );
        assert_eq!(
            AsyncKafkaProducer::new(config)
                .unwrap()
                .validate_topic(topic, timeout),
            expected
        );
    }

    #[test]
    fn test_topic_validation_existing_physical_topic() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("physical-topic", 1, 1).unwrap();
        let config = KafkaConfig::new_producer_config(vec![cluster.bootstrap_servers()], None);

        assert_topic_validation(
            config,
            Topic::new("physical-topic"),
            Duration::from_secs(5),
            Ok(()),
        );
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
        );

        assert_topic_validation(
            config,
            Topic::new("unknown-topic"),
            Duration::from_secs(5),
            Err(KafkaError::MetadataFetch(
                RDKafkaErrorCode::UnknownTopicOrPartition,
            )),
        );
    }

    #[test]
    fn test_topic_validation_metadata_fetch_failure() {
        let config = KafkaConfig::new_producer_config(Vec::new(), None);
        assert_topic_validation(
            config,
            Topic::new("physical-topic"),
            Duration::from_millis(100),
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
        );
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
        let cluster = MockCluster::new(1).unwrap();
        let topic = Topic::new("test");
        let destination = TopicOrPartition::Topic(topic);
        let configuration =
            KafkaConfig::new_producer_config(vec![cluster.bootstrap_servers()], None);
        let producer = AsyncKafkaProducer::new(configuration).unwrap();

        let payload = KafkaPayload::new(None, None, Some("asdf".as_bytes().to_vec()));
        producer.produce(&destination, payload).await.unwrap();
    }

    #[tokio::test]
    async fn test_async_producer_with_error() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.request_errors(
            RDKafkaApiKey::Produce,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE],
        );
        let topic = Topic::new("test");
        let destination = TopicOrPartition::Topic(topic);
        let configuration =
            KafkaConfig::new_producer_config(vec![cluster.bootstrap_servers()], None);
        let producer = AsyncKafkaProducer::new(configuration).unwrap();

        let payload = KafkaPayload::new(None, None, Some("asdf".as_bytes().to_vec()));
        let result = producer.produce(&destination, payload).await;
        assert!(matches!(
            result,
            Err(ProducerError::Kafka(KafkaError::MessageProduction(
                RDKafkaErrorCode::MessageSizeTooLarge
            )))
        ));
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

    #[tokio::test]
    async fn test_async_enqueue_error_retains_queue_full_error() {
        let producer = AsyncKafkaProducer::new(queue_full_configuration()).unwrap();
        let destination = TopicOrPartition::Topic(Topic::new("test"));

        // Keep the first future unpolled to verify that enqueueing is eager.
        let _first = producer.produce(
            &destination,
            KafkaPayload::new(None, None, Some(b"first".to_vec())),
        );
        let second = producer.produce(
            &destination,
            KafkaPayload::new(None, None, Some(b"second".to_vec())),
        );
        assert!(matches!(
            second.await,
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
    fn test_produce_record() {
        let cluster = MockCluster::new(1).unwrap();
        let config = KafkaConfig::new_producer_config(vec![cluster.bootstrap_servers()], None);
        let (producer, reports) = callback_producer(config);

        {
            let key = 42_u128.to_be_bytes();
            let payload = *b"payload";
            let headers = OwnedHeaders::new().insert(Header {
                key: "version",
                value: Some("1"),
            });
            producer
                .produce_record(
                    BaseRecord::to("test")
                        .key(key.as_slice())
                        .payload(payload.as_slice())
                        .headers(headers),
                )
                .unwrap();
        }
        // The borrowed buffers need not stay in scope while waiting for delivery.
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

    /// Records every counter increment with its key.
    #[derive(Clone, Default)]
    struct CapturingRecorder(Arc<Mutex<Vec<(Key, f64)>>>);

    struct CapturingMetric(Key, Arc<Mutex<Vec<(Key, f64)>>>);

    impl CapturingMetric {
        fn push(&self, value: f64) {
            self.1.lock().unwrap().push((self.0.clone(), value));
        }
    }

    impl CounterFn for CapturingMetric {
        fn increment(&self, value: u64) {
            self.push(value as f64);
        }

        fn absolute(&self, value: u64) {
            self.push(value as f64);
        }
    }

    impl Recorder for CapturingRecorder {
        fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
            Counter::from_arc(Arc::new(CapturingMetric(key.clone(), self.0.clone())))
        }

        fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    impl CapturingRecorder {
        /// Returns `(labels, value)` for every value recorded under `name`.
        fn values(&self, name: &str) -> Vec<(Vec<(String, String)>, f64)> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter(|(key, _)| key.name() == name)
                .map(|(key, value)| {
                    let mut labels: Vec<_> = key
                        .labels()
                        .map(|label| (label.key().to_owned(), label.value().to_owned()))
                        .collect();
                    labels.sort_unstable();
                    (labels, *value)
                })
                .collect()
        }
    }

    fn labels(labels: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut labels: Vec<_> = labels
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        labels.sort_unstable();
        labels
    }

    #[test]
    fn test_custom_context_records_enqueue_errors() {
        let config = KafkaConfig::new_producer_config(
            Vec::new(),
            Some(HashMap::from([
                ("queue.buffering.max.messages".to_string(), "1".to_string()),
                ("message.timeout.ms".to_string(), "5000".to_string()),
                ("client.id".to_string(), "my-producer".to_string()),
            ])),
        );
        let (producer, _reports) = callback_producer(config);
        let destination = TopicOrPartition::Topic(Topic::new("test"));

        let recorder = CapturingRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            producer
                .produce(
                    &destination,
                    KafkaPayload::new(None, None, Some(b"accepted".to_vec())),
                )
                .unwrap();
            producer
                .produce(
                    &destination,
                    KafkaPayload::new(None, None, Some(b"no".to_vec())),
                )
                .unwrap_err();
        });

        // The context replaces the delivery callback, so only the enqueue error
        // is counted.
        assert_eq!(
            recorder.values("arroyo.producer.produce_status"),
            vec![(
                labels(&[
                    ("status", "error"),
                    ("code", "MessageProduction(QueueFull (Local: Queue full))"),
                    ("topic", "test"),
                    ("producer_name", "my-producer"),
                ]),
                1.0
            )]
        );
    }
}
