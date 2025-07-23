// An example of using the RunTask and Produce strategies together.
// inspired by https://github.com/getsentry/arroyo/blob/main/examples/transform_and_produce/script.py
// This creates a consumer that reads from a topic test_in, reverses the string message,
// and then produces it to topic test_out.
extern crate sentry_arroyo;

use rdkafka::message::ToBytes;
use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::producer::AsyncKafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::kafka::InitialOffset;
use sentry_arroyo::processing::strategies::noop::Noop;
use sentry_arroyo::processing::strategies::produce::Produce;
use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use sentry_arroyo::processing::strategies::{
    ProcessingStrategy, ProcessingStrategyFactory, SubmitError,
};
use sentry_arroyo::processing::StreamProcessor;
use sentry_arroyo::types::{Message, Topic, TopicOrPartition};

fn reverse_string(
    message: Message<KafkaPayload>,
) -> Result<Message<KafkaPayload>, SubmitError<KafkaPayload>> {
    let value = message.payload();
    let payload = value.payload().unwrap();
    let str_payload = std::str::from_utf8(payload).unwrap();
    let result_str = str_payload.chars().rev().collect::<String>();

    println!("transforming value: {:?} -> {:?}", str_payload, &result_str);

    let result = KafkaPayload::new(
        value.key().cloned(),
        value.headers().cloned(),
        Some(result_str.to_bytes().to_vec()),
    );
    Ok(message.replace(result))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    struct ReverseStringAndProduceStrategyFactory {
        concurrency: ConcurrencyConfig,
        config: KafkaConfig,
        topic: Topic,
    }
    impl ProcessingStrategyFactory<KafkaPayload> for ReverseStringAndProduceStrategyFactory {
        fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
            let producer = AsyncKafkaProducer::new(self.config.clone());
            let topic = TopicOrPartition::Topic(self.topic);
            let reverse_string_and_produce_strategy = RunTask::new(
                reverse_string,
                Produce::new(Noop {}, producer, &self.concurrency, topic),
            );
            Box::new(reverse_string_and_produce_strategy)
        }
    }

    let config = KafkaConfig::new_consumer_config(
        vec!["0.0.0.0:9092".to_string()],
        "my_group".to_string(),
        InitialOffset::Latest,
        false,
        30_000,
        None,
    );

    let factory = ReverseStringAndProduceStrategyFactory {
        concurrency: ConcurrencyConfig::new(5),
        config: config.clone(),
        topic: Topic::new("test_out"),
    };

    let processor = StreamProcessor::with_kafka(config, factory, Topic::new("test_in"), None);
    println!("running processor. transforming from test_in to test_out");
    processor.run().unwrap();
}
