//! Metrics emitted by Arroyo.
//!
//! Arroyo records metrics through the [`metrics-rs`](facade) facade. Applications
//! are responsible for installing the recorder/exporter used by the entire process.
//! The legacy [`Recorder`] and [`StatsdRecorder`] APIs remain available for
//! compatibility and are adapted to the same facade.

mod globals;
mod macros;
mod statsd;
mod types;

use std::fmt::Display;

pub use ::metrics as facade;
pub use globals::*;
pub use statsd::*;
pub use types::*;

#[doc(hidden)]
pub fn metric_name(value: &dyn Display) -> String {
    value.to_string()
}

#[doc(hidden)]
pub fn metric_label(value: &dyn Display) -> String {
    value.to_string()
}

#[doc(hidden)]
pub fn counter_value(value: impl Into<MetricValue>) -> u64 {
    metric_counter_value(value.into())
}

#[doc(hidden)]
pub fn gauge_value(value: impl Into<MetricValue>) -> f64 {
    metric_number(value.into())
}

#[doc(hidden)]
pub fn timer_milliseconds(value: impl Into<MetricValue>) -> f64 {
    metric_milliseconds(value.into())
}
