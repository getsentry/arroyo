extern crate sentry_arroyo;

use std::time::Duration;

use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::kafka::InitialOffset;
use sentry_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use sentry_arroyo::processing::strategies::{ProcessingStrategy, ProcessingStrategyFactory};
use sentry_arroyo::processing::StreamProcessor;
use sentry_arroyo::types::Topic;

struct TestFactory {}
impl ProcessingStrategyFactory<KafkaPayload> for TestFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        Box::new(CommitOffsets::new(Duration::from_secs(1)))
    }
}

fn main() {
    tracing_subscriber::fmt::init();

    let config = KafkaConfig::new_consumer_config(
        vec!["127.0.0.1:9092".to_string()],
        "my_group".to_string(),
        InitialOffset::Latest,
        false,
        30_000,
        None,
    );

    let mut processor = StreamProcessor::with_kafka(
        config,
        TestFactory {},
        Topic::new("test_static"),
        None,
        None,
    );

    for _ in 0..20 {
        processor.run_once().unwrap();
    }
}
