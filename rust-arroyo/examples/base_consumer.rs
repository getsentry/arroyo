extern crate sentry_arroyo;

use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::InitialOffset;
use sentry_arroyo::backends::kafka::KafkaConsumer;
use sentry_arroyo::backends::AssignmentCallbacks;
use sentry_arroyo::backends::CommitOffsets;
use sentry_arroyo::backends::Consumer;
use sentry_arroyo::types::{Partition, Topic};
use std::collections::HashMap;

struct EmptyCallbacks {}
impl AssignmentCallbacks for EmptyCallbacks {
    fn on_assign(&self, _: HashMap<Partition, u64>) {}
    fn on_revoke<C: CommitOffsets>(&self, _: C, _: Vec<Partition>) {}
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

    let topic = Topic::new("test_static");
    let mut consumer = KafkaConsumer::new(config, &[topic], EmptyCallbacks {}).unwrap();
    println!("Subscribed");
    for _ in 0..20 {
        println!("Polling");
        let res = consumer.poll(None);
        if let Some(x) = res.unwrap() {
            println!("MSG {:?}", x)
        }
    }
}
