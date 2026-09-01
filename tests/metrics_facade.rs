use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};

#[derive(Debug, PartialEq)]
enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(f64),
}

#[derive(Debug, PartialEq)]
struct RecordedMetric {
    name: String,
    labels: BTreeMap<String, String>,
    value: MetricValue,
}

#[derive(Clone)]
struct RecordingHandle {
    name: String,
    labels: BTreeMap<String, String>,
    metrics: Arc<Mutex<Vec<RecordedMetric>>>,
}

impl RecordingHandle {
    fn new(key: &Key, metrics: Arc<Mutex<Vec<RecordedMetric>>>) -> Self {
        Self {
            name: key.name().to_owned(),
            labels: key
                .labels()
                .map(|label| (label.key().to_owned(), label.value().to_owned()))
                .collect(),
            metrics,
        }
    }

    fn record(&self, value: MetricValue) {
        self.metrics.lock().unwrap().push(RecordedMetric {
            name: self.name.clone(),
            labels: self.labels.clone(),
            value,
        });
    }
}

impl CounterFn for RecordingHandle {
    fn increment(&self, value: u64) {
        self.record(MetricValue::Counter(value));
    }

    fn absolute(&self, value: u64) {
        self.record(MetricValue::Counter(value));
    }
}

impl GaugeFn for RecordingHandle {
    fn increment(&self, value: f64) {
        self.record(MetricValue::Gauge(value));
    }

    fn decrement(&self, value: f64) {
        self.record(MetricValue::Gauge(-value));
    }

    fn set(&self, value: f64) {
        self.record(MetricValue::Gauge(value));
    }
}

impl HistogramFn for RecordingHandle {
    fn record(&self, value: f64) {
        self.record(MetricValue::Histogram(value));
    }
}

#[derive(Default)]
struct RecordingRecorder {
    metrics: Arc<Mutex<Vec<RecordedMetric>>>,
}

impl Recorder for RecordingRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(RecordingHandle::new(
            key,
            Arc::clone(&self.metrics),
        )))
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(RecordingHandle::new(
            key,
            Arc::clone(&self.metrics),
        )))
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(RecordingHandle::new(
            key,
            Arc::clone(&self.metrics),
        )))
    }
}

#[test]
fn arroyo_and_application_metrics_use_the_same_metrics_recorder() {
    let recorder = RecordingRecorder::default();
    let recorded = Arc::clone(&recorder.metrics);

    metrics::with_local_recorder(&recorder, || {
        metrics::counter!("application.counter").increment(2);

        sentry_arroyo::counter!(
            "arroyo.test.counter",
            3,
            "status" => "ok",
            "partition" => 7
        );
        sentry_arroyo::gauge!("arroyo.test.gauge", -2, "producer_name" => "producer");

        let timer_name = "arroyo.test.timer".to_owned();
        sentry_arroyo::timer!(&timer_name, Duration::from_micros(1_234_567));

        sentry_arroyo::metrics::record_metric(sentry_arroyo::metric!(
            Counter: "legacy.counter", 4
        ));
    });

    assert_eq!(
        *recorded.lock().unwrap(),
        vec![
            RecordedMetric {
                name: "application.counter".to_owned(),
                labels: BTreeMap::new(),
                value: MetricValue::Counter(2),
            },
            RecordedMetric {
                name: "arroyo.test.counter".to_owned(),
                labels: BTreeMap::from([
                    ("partition".to_owned(), "7".to_owned()),
                    ("status".to_owned(), "ok".to_owned()),
                ]),
                value: MetricValue::Counter(3),
            },
            RecordedMetric {
                name: "arroyo.test.gauge".to_owned(),
                labels: BTreeMap::from([("producer_name".to_owned(), "producer".to_owned(),)]),
                value: MetricValue::Gauge(-2.0),
            },
            RecordedMetric {
                name: "arroyo.test.timer".to_owned(),
                labels: BTreeMap::new(),
                value: MetricValue::Histogram(1_234.0),
            },
            RecordedMetric {
                name: "legacy.counter".to_owned(),
                labels: BTreeMap::new(),
                value: MetricValue::Counter(4),
            },
        ]
    );
}
