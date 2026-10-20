use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use sentry_arroyo::metrics::configure_scope;
use sentry_arroyo::processing::dlq::BufferedMessages;
use sentry_arroyo::processing::strategies::noop::Noop;
use sentry_arroyo::processing::strategies::reduce::Reduce;
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use sentry_arroyo::types::{BrokerMessage, Message, Partition, Topic};

const REDUCE_MODULE: &str = "sentry_arroyo::processing::strategies::reduce";
const DLQ_MODULE: &str = "sentry_arroyo::processing::dlq";

struct RecordedMetric {
    kind: &'static str,
    key: Key,
    target: String,
    module_path: Option<String>,
}

struct CapturingRecorder(Sender<RecordedMetric>);

impl CapturingRecorder {
    fn capture(&self, kind: &'static str, key: &Key, metadata: &Metadata<'_>) {
        assert_eq!(metadata.level(), &metrics::Level::INFO);
        self.0
            .send(RecordedMetric {
                kind,
                key: key.clone(),
                target: metadata.target().to_owned(),
                module_path: metadata.module_path().map(str::to_owned),
            })
            .unwrap();
    }
}

impl Recorder for CapturingRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        self.capture("counter", key, metadata);
        Counter::noop()
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        self.capture("gauge", key, metadata);
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        self.capture("histogram", key, metadata);
        Histogram::noop()
    }
}

fn assert_metric(
    recorded: &[RecordedMetric],
    kind: &str,
    name: &str,
    labels: &[(&str, &str)],
    source: &str,
) {
    let recorded = recorded
        .iter()
        .find(|metric| metric.key.name() == name)
        .unwrap_or_else(|| panic!("metric not registered: {name}"));
    assert_eq!(recorded.kind, kind);
    assert_eq!(recorded.target, source);
    assert_eq!(recorded.module_path.as_deref(), Some(source));
    let mut actual: Vec<_> = recorded
        .key
        .labels()
        .map(|label| (label.key(), label.value()))
        .collect();
    let mut expected = labels.to_vec();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected, "{name}");
}

#[test]
fn startup_scope_tags_are_automatic_and_preserve_metadata() {
    let (sender, receiver) = mpsc::channel();
    metrics::set_global_recorder(CapturingRecorder(sender)).unwrap();
    configure_scope(|scope| {
        scope.set_tag("application", "my-service");
        scope.set_tag("partition_id", "scope-default");
    })
    .unwrap();

    // Tags apply across threads.
    thread::spawn(|| {
        let message = BrokerMessage::new(
            (),
            Partition::new(Topic::new("events"), 3),
            0,
            chrono::Utc::now(),
        );
        let mut buffer = BufferedMessages::new(Some(1));
        buffer.append(&message);
        buffer.append(&message);

        let mut reduce = Reduce::new(
            Noop {},
            Arc::new(|(), _: Message<()>| Ok(())),
            Arc::new(|| ()),
            1,
            Duration::from_secs(1),
            |_| 1,
        );
        reduce
            .submit(Message::new_any_message((), Default::default()))
            .unwrap();
        reduce.poll().unwrap();
    })
    .join()
    .unwrap();

    // Application metrics are unaffected, even with the Arroyo prefix.
    metrics::counter!("arroyo.client_metric", "route" => "ingest").increment(1);
    let recorded: Vec<_> = receiver.try_iter().collect();
    let tags = [
        ("application", "my-service"),
        ("partition_id", "scope-default"),
    ];
    assert_metric(
        &recorded,
        "gauge",
        "arroyo.consumer.dlq_buffer.assigned_partitions",
        &tags,
        DLQ_MODULE,
    );
    assert_metric(
        &recorded,
        "counter",
        "arroyo.consumer.dlq_buffer.exceeded",
        &[("application", "my-service"), ("partition_id", "3")],
        DLQ_MODULE,
    );
    let mut histogram_tags = tags.to_vec();
    histogram_tags.push(("flush_reason", "size"));
    assert_metric(
        &recorded,
        "histogram",
        "arroyo.strategies.reduce.batch_time.ms",
        &histogram_tags,
        REDUCE_MODULE,
    );
    assert_metric(
        &recorded,
        "counter",
        "arroyo.client_metric",
        &[("route", "ingest")],
        module_path!(),
    );
}
