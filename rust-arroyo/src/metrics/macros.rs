/// Create a [`Metric`].
///
/// Instead of creating metrics directly, it is recommended to immediately record
/// metrics using the [`counter!`], [`gauge!`] or [`timer!`] macros.
///
/// This is the recommended way to create a [`Metric`], as the
/// implementation details of it might change.
///
/// [`Metric`]: crate::metrics::Metric
#[macro_export]
macro_rules! metric {
    ($ty:ident: $key:expr, $value:expr
        $(, $($tag_key:expr => $tag_val:expr),*)?
    ) => {{
        $crate::metrics::Metric {
            key: &$key,
            ty: $crate::metrics::MetricType::$ty,

            tags: &[
                $($(($tag_key, &$tag_val),)*)?
            ],
            value: $value.into(),

            __private: (),
        }
    }};
}

/// Increments a counter using the application's metrics recorder.
#[macro_export]
macro_rules! counter {
    ($key:literal) => {{
        $crate::__metrics::counter!($key).increment(1);
    }};
    ($key:expr) => {{
        let name = $crate::metrics::metric_name(&$key);
        $crate::__metrics::counter!(name).increment(1);
    }};
    ($key:literal, $value:expr $(,)?) => {{
        let value = $crate::metrics::counter_value($value);
        $crate::__metrics::counter!($key).increment(value);
    }};
    ($key:expr, $value:expr $(,)?) => {{
        let name = $crate::metrics::metric_name(&$key);
        let value = $crate::metrics::counter_value($value);
        $crate::__metrics::counter!(name).increment(value);
    }};
    ($key:literal, $value:expr, $($tag_key:expr => $tag_val:expr),+ $(,)?) => {{
        let value = $crate::metrics::counter_value($value);
        let labels = ::std::vec![
            $($crate::__metrics::Label::new(
                $tag_key,
                $crate::metrics::metric_label(&$tag_val),
            )),+
        ];
        $crate::__metrics::counter!($key, labels).increment(value);
    }};
    ($key:expr, $value:expr, $($tag_key:expr => $tag_val:expr),+ $(,)?) => {{
        let name = $crate::metrics::metric_name(&$key);
        let value = $crate::metrics::counter_value($value);
        let labels = ::std::vec![
            $($crate::__metrics::Label::new(
                $tag_key,
                $crate::metrics::metric_label(&$tag_val),
            )),+
        ];
        $crate::__metrics::counter!(name, labels).increment(value);
    }};
}

/// Sets a gauge using the application's metrics recorder.
#[macro_export]
macro_rules! gauge {
    ($key:literal, $value:expr $(,)?) => {{
        let value = $crate::metrics::gauge_value($value);
        $crate::__metrics::gauge!($key).set(value);
    }};
    ($key:expr, $value:expr $(,)?) => {{
        let name = $crate::metrics::metric_name(&$key);
        let value = $crate::metrics::gauge_value($value);
        $crate::__metrics::gauge!(name).set(value);
    }};
    ($key:literal, $value:expr, $($tag_key:expr => $tag_val:expr),+ $(,)?) => {{
        let value = $crate::metrics::gauge_value($value);
        let labels = ::std::vec![
            $($crate::__metrics::Label::new(
                $tag_key,
                $crate::metrics::metric_label(&$tag_val),
            )),+
        ];
        $crate::__metrics::gauge!($key, labels).set(value);
    }};
    ($key:expr, $value:expr, $($tag_key:expr => $tag_val:expr),+ $(,)?) => {{
        let name = $crate::metrics::metric_name(&$key);
        let value = $crate::metrics::gauge_value($value);
        let labels = ::std::vec![
            $($crate::__metrics::Label::new(
                $tag_key,
                $crate::metrics::metric_label(&$tag_val),
            )),+
        ];
        $crate::__metrics::gauge!(name, labels).set(value);
    }};
}

/// Records a millisecond timer using the application's metrics recorder.
#[macro_export]
macro_rules! timer {
    ($key:literal, $value:expr $(,)?) => {{
        let value = $crate::metrics::timer_milliseconds($value);
        $crate::__metrics::histogram!($key).record(value);
    }};
    ($key:expr, $value:expr $(,)?) => {{
        let name = $crate::metrics::metric_name(&$key);
        let value = $crate::metrics::timer_milliseconds($value);
        $crate::__metrics::histogram!(name).record(value);
    }};
    ($key:literal, $value:expr, $($tag_key:expr => $tag_val:expr),+ $(,)?) => {{
        let value = $crate::metrics::timer_milliseconds($value);
        let labels = ::std::vec![
            $($crate::__metrics::Label::new(
                $tag_key,
                $crate::metrics::metric_label(&$tag_val),
            )),+
        ];
        $crate::__metrics::histogram!($key, labels).record(value);
    }};
    ($key:expr, $value:expr, $($tag_key:expr => $tag_val:expr),+ $(,)?) => {{
        let name = $crate::metrics::metric_name(&$key);
        let value = $crate::metrics::timer_milliseconds($value);
        let labels = ::std::vec![
            $($crate::__metrics::Label::new(
                $tag_key,
                $crate::metrics::metric_label(&$tag_val),
            )),+
        ];
        $crate::__metrics::histogram!(name, labels).record(value);
    }};
}

#[macro_export]
#[doc(hidden)]
macro_rules! __record_metric {
    ($($tt:tt)+) => {{
        $crate::metrics::record_metric($crate::metric!($($tt)+));
    }};
}
