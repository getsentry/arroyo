/// Pull-based version of transform_and_produce.
///
/// Pipeline:
///   KafkaSource → apply_stage(reverse) → on_ok(produce) → on_error(dlq) → commit
extern crate sentry_arroyo;

use std::time::Duration;

use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::kafka::InitialOffset;
use sentry_arroyo::processing::strategies::offset_tracker::OffsetTracker;
use sentry_arroyo::processing::stream::{
    Envelope, KafkaProducerHandler, KafkaSource, LogErrorHandler, PipelineExt,
    Stage, StageError,
};
use sentry_arroyo::types::{Topic, TopicOrPartition};

/// A Stage that reverses the string payload.
struct ReverseStage;

impl Stage for ReverseStage {
    type In = KafkaPayload;
    type Out = KafkaPayload;

    async fn process(
        &self,
        envelope: Envelope<KafkaPayload>,
    ) -> Result<Envelope<KafkaPayload>, StageError> {
        Ok(envelope.map_payload(|p| {
            let bytes = p.payload().unwrap();
            let s = std::str::from_utf8(bytes).unwrap();
            let reversed: String = s.chars().rev().collect();
            println!("transforming: {:?} -> {:?}", s, reversed);
            KafkaPayload::new(p.key().cloned(), p.headers().cloned(), Some(reversed.into_bytes()))
        }))
    }

    fn name(&self) -> &'static str {
        "reverse_string"
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // --- Construct components ---
    let consumer_config = KafkaConfig::new_consumer_config(
        vec!["0.0.0.0:9092".to_string()],
        "my_group".to_string(),
        InitialOffset::Latest,
        false,
        30_000,
        None,
    );
    let source = KafkaSource::new(consumer_config, &[Topic::new("test_in")]);

    let producer_config = KafkaConfig::new_producer_config(
        vec!["0.0.0.0:9092".to_string()],
        None,
    );
    let producer = KafkaProducer::new(producer_config);
    let produce_handler =
        KafkaProducerHandler::new(producer, TopicOrPartition::Topic(Topic::new("test_out")));

    // Use LogErrorHandler for this example (no DLQ topic configured).
    // In production, replace with:
    //   DlqErrorHandler::new(dlq_producer, TopicOrPartition::Topic(dlq_topic))
    let error_handler = LogErrorHandler;

    let mut tracker = OffsetTracker::new(Duration::from_secs(1));

    let reverse = ReverseStage;

    // --- Wire pipeline ---
    let result = source
        .stream()
        .apply_stage(&reverse)
        .on_ok(&produce_handler)
        .on_error(&error_handler)
        .commit(&mut tracker)
        .await;

    if let Err(e) = result {
        tracing::error!("Pipeline stopped: {}", e);
    }
}
