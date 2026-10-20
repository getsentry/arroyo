// How Relay wires up a self-reloading producer. Relay passes opaque bytes from
// options-automator at startup and on changes; Arroyo validates and applies them.
//
// Run with: cargo run --example reloading_producer
extern crate sentry_arroyo;

use std::time::Duration;

use sentry_arroyo::backends::kafka::config_blob::ProducerSelector;
use sentry_arroyo::backends::kafka::reloading_producer::{
    ReloadConfig, ReloadOutcome, ReloadingKafkaProducer,
};
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::Producer;
use sentry_arroyo::types::{Topic, TopicOrPartition};

/// Stands in for the opaque bytes options-automator hands to Relay.
fn config_blob(acks: &str) -> Vec<u8> {
    format!(
        r#"{{
            "clusters": {{
                "events": {{
                    "config": {{"bootstrap.servers": "127.0.0.1:9092"}},
                    "producer_config": {{"compression.type": "lz4"}}
                }}
            }},
            "topics": {{
                "ingest-events": {{
                    "cluster": "events",
                    "logical_topic": "ingest-events",
                    "producer_config": {{"acks": "{acks}"}},
                    "producer_config_overrides": {{"relay": {{"linger.ms": "5"}}}}
                }}
            }}
        }}"#
    )
    .into_bytes()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // `app` picks which producer_config_overrides entry applies. Asserting the
    // logical topic makes Arroyo reject a blob that repoints this topic at
    // something Relay does not expect.
    let selector =
        ProducerSelector::new("ingest-events", "relay").expect_logical_topic("ingest-events");

    let settings = ReloadConfig {
        // How long the reload worker flushes the old client. Off the produce
        // path entirely.
        drain_timeout: Duration::from_secs(10),
        // Cap on messages held during a drain. Unused here since
        // `ignore_key_ordering` is set, so nothing is ever buffered.
        max_buffered_messages: 10_000,
        // Spread swaps across the fleet so pods do not all stall at once.
        jitter: Duration::from_secs(2),
        // Require the new client to reach a broker before swapping it in, so a
        // typo'd address cannot retire a working producer.
        probe_timeout: Duration::from_secs(5),
        probe_retry_interval: Duration::from_secs(5),
        // Relay keys messages to spread them across partitions, not to order
        // them, so the swap need not drain the old client first.
        ignore_key_ordering: true,
    };

    let producer = ReloadingKafkaProducer::new(&config_blob("all"), selector, settings)
        .expect("initial config should be valid");

    let destination = TopicOrPartition::Topic(Topic::new("ingest-events"));

    // Clones share one client and all see reloads.
    let worker = producer.clone();
    tokio::task::spawn_blocking(move || {
        for i in 0..20 {
            let key = (i % 2 == 0).then(|| b"some-key".to_vec());
            let payload = KafkaPayload::new(key, None, Some(format!("message-{i}").into_bytes()));

            match worker.produce(&destination, payload) {
                Ok(()) => {}
                Err(error) => tracing::error!(%error, "produce failed"),
            }

            std::thread::sleep(Duration::from_millis(100));
        }
    });

    // Simulate Relay's options-automator callback.
    for (attempt, blob) in [
        config_blob("all"),     // same as what is running: no swap
        config_blob("1"),       // a real change: swap
        b"{ not json".to_vec(), // rejected, old client keeps running
    ]
    .into_iter()
    .enumerate()
    {
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Returns immediately: the worker does the probing and swapping.
        match producer.push_config(&blob) {
            Ok(ReloadOutcome::Accepted) => {
                tracing::info!(attempt, "config accepted, rolling out in the background")
            }
            Ok(ReloadOutcome::Unchanged) => tracing::info!(attempt, "config unchanged"),
            // A bad config leaves the current producer running.
            Err(error) => tracing::error!(attempt, %error, "config rejected, still on old config"),
        }
    }

    // Give the worker time to finish rolling out the accepted config.
    tokio::time::sleep(Duration::from_secs(6)).await;
    tracing::info!(
        generation = producer.generation(),
        "final config generation"
    );
}
