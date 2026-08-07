/// Pull-based version of transform_and_produce.
///
/// Pipeline:
///   KafkaSource → apply(reverse) → on_next(produce) → on_reject(log) → commit
///
/// `PipelineRunner::run()` handles the rebalance restart loop —
/// the closure is called once per partition assignment with fresh
/// stages, handlers, and tracker.
extern crate sentry_arroyo;

use std::time::Duration;

use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::kafka::InitialOffset;
use sentry_arroyo::processing::stream::{
    KafkaProducerHandler, KafkaSource, LogHandler, OffsetTracker, PipelineEnvelope, PipelineExt,
    PipelineRunner, Stage, StageResult,
};
use sentry_arroyo::types::{Topic, TopicOrPartition};

/// A Stage that reverses the string payload.
struct ReverseStage;

impl Stage for ReverseStage {
    type In = KafkaPayload;
    type Out = KafkaPayload;

    async fn process(&self, envelope: PipelineEnvelope<KafkaPayload>) -> StageResult<KafkaPayload> {
        let reversed = envelope.map_payload(|p| {
            let bytes = p.payload().unwrap();
            let s = std::str::from_utf8(bytes).unwrap();
            let reversed: String = s.chars().rev().collect();
            println!("transforming: {:?} -> {:?}", s, reversed);
            KafkaPayload::new(
                p.key().cloned(),
                p.headers().cloned(),
                Some(reversed.into_bytes()),
            )
        });

        StageResult::Emit(reversed)
    }

    fn name(&self) -> &'static str {
        "reverse_string"
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let consumer_config = KafkaConfig::new_consumer_config(
        vec!["0.0.0.0:9092".to_string()],
        "my_group".to_string(),
        InitialOffset::Latest,
        false,
        30_000,
        None,
    );
    let source = KafkaSource::new(consumer_config, &[Topic::new("test_in")]);

    let producer_config = KafkaConfig::new_producer_config(vec!["0.0.0.0:9092".to_string()], None);
    // The pipeline reads left to right, top to bottom:
    //   stream                     — async stream of Kafka messages
    //     .apply(&stage)           — transform/filter/batch each message
    //     .on_next(&handler)       — side-effect on successful items (produce, upload)
    //     .on_reject(&handler)     — handle rejected items (DLQ, log)
    //     .commit(&mut tracker)    — track offsets, flush on interval
    //
    // PipelineRunner::run() handles the lifecycle:
    //   - calls the closure once per partition assignment
    //   - on rebalance: closure is called again with a fresh stream
    //   - on shutdown or stream end: exits
    let result = PipelineRunner::run(&source, |stream, committer| async {
        let reverse = ReverseStage;
        let producer = KafkaProducer::new(producer_config.clone());
        let produce_handler =
            KafkaProducerHandler::new(producer, TopicOrPartition::Topic(Topic::new("test_out")));
        let error_handler = LogHandler;
        let mut tracker = OffsetTracker::new(Duration::from_secs(1), committer);

        stream
            .apply(&reverse)
            .on_next(&produce_handler)
            .on_reject(&error_handler)
            .commit(&mut tracker)
            .await
    })
    .await;

    if let Err(e) = result {
        tracing::error!("Pipeline stopped: {}", e);
    }
}
