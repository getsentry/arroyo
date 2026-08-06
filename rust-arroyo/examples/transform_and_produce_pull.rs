/// Pull-based version of transform_and_produce.
///
/// Pipeline:
///   KafkaSource → apply(reverse) → on_next(produce) → on_reject(dlq) → commit
extern crate sentry_arroyo;

use std::time::Duration;

use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::kafka::InitialOffset;
use sentry_arroyo::processing::strategies::offset_tracker::OffsetTracker;
use sentry_arroyo::processing::stream::{
    PipelineEnvelope, KafkaProducerHandler, KafkaSource, LogHandler, PipelineExt,
    Stage, StageResult,
};
use sentry_arroyo::types::{Topic, TopicOrPartition};

/// A Stage that reverses the string payload.
struct ReverseStage;

impl Stage for ReverseStage {
    type In = KafkaPayload;
    type Out = KafkaPayload;

    async fn process(
        &self,
        envelope: PipelineEnvelope<KafkaPayload>,
    ) -> StageResult<KafkaPayload> {
        StageResult::Emit(envelope.map_payload(|p| {
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

    let error_handler = LogHandler;
    let mut tracker = OffsetTracker::new(Duration::from_secs(1), &source);
    let reverse = ReverseStage;

    // --- Wire pipeline ---
    let result = source
        .stream()
        .apply(&reverse)
        .on_next(&produce_handler)
        .on_reject(&error_handler)
        .commit(&mut tracker)
        .await;

    if let Err(e) = result {
        tracing::error!("Pipeline stopped: {}", e);
    }
}
