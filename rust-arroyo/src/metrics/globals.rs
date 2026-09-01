#![allow(clippy::mutable_key_type)]

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ::metrics::{
    Counter as MetricsCounter, CounterFn, Gauge as MetricsGauge, GaugeFn,
    Histogram as MetricsHistogram, HistogramFn, Key, KeyName, Label, Level, Metadata,
    Recorder as MetricsRecorder, SharedString, Unit,
};
use parking_lot::Mutex;

use super::{Metric, MetricType, MetricValue};

/// A compatibility recorder for Arroyo's original metrics API.
///
/// New applications should install a [`metrics-rs`](super::facade) recorder
/// directly. Recorders installed through [`init`] are adapted to that same facade.
pub trait Recorder {
    /// Records the given metric.
    fn record_metric(&self, metric: Metric<'_>);
}

impl<T: Recorder + ?Sized> Recorder for Box<T> {
    fn record_metric(&self, metric: Metric<'_>) {
        (**self).record_metric(metric)
    }
}

/// Installs a legacy Arroyo recorder as the process-wide `metrics-rs` recorder.
///
/// This function can only succeed when no other global `metrics-rs` recorder has
/// been installed. New applications should install their recorder through their
/// chosen `metrics-rs` exporter instead.
pub fn init<R: Recorder + Send + Sync + 'static>(recorder: R) -> Result<(), R> {
    match ::metrics::set_global_recorder(LegacyRecorderAdapter::new(recorder)) {
        Ok(()) => Ok(()),
        Err(error) => Err(error.into_inner().into_recorder()),
    }
}

/// Records a compatibility [`Metric`] through the active `metrics-rs` recorder.
pub fn record_metric(metric: Metric<'_>) {
    static METADATA: Metadata<'static> =
        Metadata::new("sentry_arroyo", Level::INFO, Some(module_path!()));

    let labels = metric
        .tags
        .iter()
        .map(|(key, value)| Label::new((*key).to_owned(), value.to_string()))
        .collect::<Vec<_>>();
    let key = Key::from_parts(metric.key.to_string(), labels);

    ::metrics::with_recorder(|recorder| match metric.ty {
        MetricType::Counter => recorder
            .register_counter(&key, &METADATA)
            .increment(metric_counter_value(metric.value)),
        MetricType::Gauge => recorder
            .register_gauge(&key, &METADATA)
            .set(metric_number(metric.value)),
        MetricType::Timer => recorder
            .register_histogram(&key, &METADATA)
            .record(metric_milliseconds(metric.value)),
    });
}

pub(crate) fn metric_counter_value(value: MetricValue) -> u64 {
    match value {
        MetricValue::I64(value) => value
            .try_into()
            .unwrap_or_else(|_| panic!("counter values must be non-negative integers")),
        MetricValue::U64(value) => value,
        MetricValue::F64(value) if value.is_finite() && value >= 0.0 && value.fract() == 0.0 => {
            assert!(value <= u64::MAX as f64, "counter value exceeds u64::MAX");
            value as u64
        }
        MetricValue::F64(_) => panic!("counter values must be non-negative integers"),
        MetricValue::Duration(value) => value
            .as_millis()
            .try_into()
            .unwrap_or_else(|_| panic!("counter value exceeds u64::MAX")),
    }
}

pub(crate) fn metric_number(value: MetricValue) -> f64 {
    match value {
        MetricValue::I64(value) => value as f64,
        MetricValue::U64(value) => value as f64,
        MetricValue::F64(value) => value,
        MetricValue::Duration(value) => value.as_millis() as f64,
    }
}

pub(crate) fn metric_milliseconds(value: MetricValue) -> f64 {
    metric_number(value)
}

struct LegacyRecorderAdapter<R> {
    recorder: Arc<R>,
    counters: Mutex<HashMap<Key, Arc<LegacyCounter<R>>>>,
    gauges: Mutex<HashMap<Key, Arc<LegacyGauge<R>>>>,
    histograms: Mutex<HashMap<Key, Arc<LegacyHistogram<R>>>>,
}

impl<R> LegacyRecorderAdapter<R> {
    fn new(recorder: R) -> Self {
        Self {
            recorder: Arc::new(recorder),
            counters: Mutex::new(HashMap::new()),
            gauges: Mutex::new(HashMap::new()),
            histograms: Mutex::new(HashMap::new()),
        }
    }

    fn into_recorder(self) -> R {
        match Arc::try_unwrap(self.recorder) {
            Ok(recorder) => recorder,
            Err(_) => unreachable!("a rejected recorder cannot have registered metric handles"),
        }
    }
}

impl<R: Recorder + Send + Sync + 'static> MetricsRecorder for LegacyRecorderAdapter<R> {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> MetricsCounter {
        let counter = self
            .counters
            .lock()
            .entry(key.to_retained())
            .or_insert_with(|| {
                Arc::new(LegacyCounter {
                    emitter: LegacyEmitter::new(key, Arc::clone(&self.recorder)),
                    value: AtomicU64::new(0),
                })
            })
            .clone();
        MetricsCounter::from_arc(counter)
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> MetricsGauge {
        let gauge = self
            .gauges
            .lock()
            .entry(key.to_retained())
            .or_insert_with(|| {
                Arc::new(LegacyGauge {
                    emitter: LegacyEmitter::new(key, Arc::clone(&self.recorder)),
                    value: Mutex::new(0.0),
                })
            })
            .clone();
        MetricsGauge::from_arc(gauge)
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> MetricsHistogram {
        let histogram = self
            .histograms
            .lock()
            .entry(key.to_retained())
            .or_insert_with(|| {
                Arc::new(LegacyHistogram {
                    emitter: LegacyEmitter::new(key, Arc::clone(&self.recorder)),
                })
            })
            .clone();
        MetricsHistogram::from_arc(histogram)
    }
}

struct LegacyEmitter<R> {
    recorder: Arc<R>,
    name: String,
    labels: Vec<(String, String)>,
}

impl<R: Recorder> LegacyEmitter<R> {
    fn new(key: &Key, recorder: Arc<R>) -> Self {
        Self {
            recorder,
            name: key.name().to_owned(),
            labels: key
                .labels()
                .map(|label| (label.key().to_owned(), label.value().to_owned()))
                .collect(),
        }
    }

    fn emit(&self, ty: MetricType, value: MetricValue) {
        let tags = self
            .labels
            .iter()
            .map(|(key, value)| (key.as_str(), value as &dyn Display))
            .collect::<Vec<_>>();

        self.recorder.record_metric(Metric {
            key: &self.name,
            ty,
            tags: &tags,
            value,
            __private: (),
        });
    }
}

struct LegacyCounter<R> {
    emitter: LegacyEmitter<R>,
    value: AtomicU64,
}

impl<R: Recorder> CounterFn for LegacyCounter<R> {
    fn increment(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
        self.emitter
            .emit(MetricType::Counter, MetricValue::U64(value));
    }

    fn absolute(&self, value: u64) {
        let previous = self.value.fetch_max(value, Ordering::Relaxed);
        if value > previous {
            self.emitter
                .emit(MetricType::Counter, MetricValue::U64(value - previous));
        }
    }
}

struct LegacyGauge<R> {
    emitter: LegacyEmitter<R>,
    value: Mutex<f64>,
}

impl<R: Recorder> GaugeFn for LegacyGauge<R> {
    fn increment(&self, value: f64) {
        let current = {
            let mut current = self.value.lock();
            *current += value;
            *current
        };
        self.emitter
            .emit(MetricType::Gauge, MetricValue::F64(current));
    }

    fn decrement(&self, value: f64) {
        let current = {
            let mut current = self.value.lock();
            *current -= value;
            *current
        };
        self.emitter
            .emit(MetricType::Gauge, MetricValue::F64(current));
    }

    fn set(&self, value: f64) {
        *self.value.lock() = value;
        self.emitter
            .emit(MetricType::Gauge, MetricValue::F64(value));
    }
}

struct LegacyHistogram<R> {
    emitter: LegacyEmitter<R>,
}

impl<R: Recorder> HistogramFn for LegacyHistogram<R> {
    fn record(&self, value: f64) {
        self.emitter
            .emit(MetricType::Timer, MetricValue::F64(value));
    }
}
