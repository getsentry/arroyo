/// Pull-based version of transform_and_produce.
///
/// Pipeline:
///   KafkaSource → apply(reverse) → on_next(produce) → on_reject(log) → commit
///
/// `PipelineRunner::run_pipeline()` handles the rebalance restart loop —
/// the build closure is called once per partition assignment with fresh
/// stages and handlers.
extern crate sentry_arroyo;

use std::time::Duration;

use futures::Stream;
use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::kafka::InitialOffset;
use sentry_arroyo::processing::stream::{
    KafkaProducerHandler, KafkaSource, LogHandler, Pipeline, PipelineEnvelope, PipelineExt,
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

struct TransformAndProducePipeline {
    reverse: ReverseStage,
    produce_handler: KafkaProducerHandler,
    error_handler: LogHandler,
}

impl Pipeline for TransformAndProducePipeline {
    type Output = KafkaPayload;

    fn stream(
        self,
        source: impl Stream<Item = StageResult<KafkaPayload>> + Send,
    ) -> impl Stream<Item = StageResult<KafkaPayload>> + Send {
        source
            .apply(self.reverse)
            .on_next(self.produce_handler)
            .on_reject(self.error_handler)
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

    let result = PipelineRunner::run(&source, Duration::from_secs(1), || {
        let producer = KafkaProducer::new(producer_config.clone()).unwrap();
        TransformAndProducePipeline {
            reverse: ReverseStage,
            produce_handler: KafkaProducerHandler::new(
                producer,
                TopicOrPartition::Topic(Topic::new("test_out")),
            ),
            error_handler: LogHandler,
        }
    })
    .await;

    if let Err(e) = result {
        tracing::error!("Pipeline stopped: {}", e);
    }
}
