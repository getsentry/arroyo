use std::sync::{Arc, Mutex};
use std::time::Duration;

use sentry_arroyo::metrics::{MetricSink, StatsdRecorder};

#[derive(Clone, Default)]
struct RecordingSink {
    metrics: Arc<Mutex<Vec<String>>>,
}

impl MetricSink for RecordingSink {
    fn emit(&self, metric: &str) {
        self.metrics.lock().unwrap().push(metric.to_owned());
    }
}

#[test]
fn legacy_statsd_recorder_is_adapted_to_the_metrics_facade() {
    let sink = RecordingSink::default();
    let recorded = Arc::clone(&sink.metrics);

    sentry_arroyo::metrics::init(StatsdRecorder::new("service", sink)).unwrap();

    sentry_arroyo::counter!("arroyo.test.counter", 3, "status" => "ok");
    sentry_arroyo::gauge!("arroyo.test.gauge", -2);
    sentry_arroyo::timer!("arroyo.test.timer", Duration::from_micros(1_234_567));

    assert_eq!(
        *recorded.lock().unwrap(),
        vec![
            "service.arroyo.test.counter:3|c|#status:ok",
            "service.arroyo.test.gauge:-2|g",
            "service.arroyo.test.timer:1234|ms",
        ]
    );
}
