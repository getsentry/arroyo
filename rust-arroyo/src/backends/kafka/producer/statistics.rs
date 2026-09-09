use rdkafka::Statistics;

const BROKER_STATES: &[&str] = &[
    "INIT",
    "DOWN",
    "CONNECT",
    "AUTH",
    "APIVERSION_QUERY",
    "AUTH_HANDSHAKE",
    "UP",
    "UPDATE",
    "TRY_CONNECT",
    "SSL_HANDSHAKE",
    "AUTH_LEGACY",
    "AUTH_REQ",
    "REAUTH",
    "UNKNOWN",
];

pub(super) fn record(stats: &Statistics, producer_name: &str) {
    for (broker_id, broker_stats) in &stats.brokers {
        record_broker_state(broker_id, &broker_stats.state, producer_name);

        record_broker_gauge(
            "arroyo.producer.librdkafka.broker_outbuf_requests",
            broker_stats.outbuf_cnt as f64,
            broker_id,
            producer_name,
        );
        record_broker_gauge(
            "arroyo.producer.librdkafka.broker_outbuf_messages",
            broker_stats.outbuf_msg_cnt as f64,
            broker_id,
            producer_name,
        );
        record_broker_gauge(
            "arroyo.producer.librdkafka.broker_waitresp_requests",
            broker_stats.waitresp_cnt as f64,
            broker_id,
            producer_name,
        );

        if let Some(connects) = broker_stats.connects {
            record_broker_gauge(
                "arroyo.producer.librdkafka.broker_connects",
                connects as f64,
                broker_id,
                producer_name,
            );
        }

        if let Some(disconnects) = broker_stats.disconnects {
            record_broker_gauge(
                "arroyo.producer.librdkafka.broker_disconnects",
                disconnects as f64,
                broker_id,
                producer_name,
            );
        }

        if broker_stats.txidle >= 0 {
            record_broker_gauge(
                "arroyo.producer.librdkafka.broker_tx_idle",
                (broker_stats.txidle / 1000) as f64,
                broker_id,
                producer_name,
            );
        }

        if broker_stats.rxidle >= 0 {
            record_broker_gauge(
                "arroyo.producer.librdkafka.broker_rx_idle",
                (broker_stats.rxidle / 1000) as f64,
                broker_id,
                producer_name,
            );
        }

        record_broker_gauge(
            "arroyo.producer.librdkafka.broker_request_timeouts",
            broker_stats.req_timeouts as f64,
            broker_id,
            producer_name,
        );

        if let Some(int_latency) = &broker_stats.int_latency {
            record_broker_gauge(
                "arroyo.producer.librdkafka.avg_int_latency",
                int_latency.avg as f64 / 1000.0,
                broker_id,
                producer_name,
            );
            record_broker_gauge(
                "arroyo.producer.librdkafka.p99_int_latency",
                int_latency.p99 as f64 / 1000.0,
                broker_id,
                producer_name,
            );
        }

        if let Some(outbuf_latency) = &broker_stats.outbuf_latency {
            record_broker_gauge(
                "arroyo.producer.librdkafka.avg_outbuf_latency",
                outbuf_latency.avg as f64 / 1000.0,
                broker_id,
                producer_name,
            );
            record_broker_gauge(
                "arroyo.producer.librdkafka.p99_outbuf_latency",
                outbuf_latency.p99 as f64 / 1000.0,
                broker_id,
                producer_name,
            );
        }

        if let Some(rtt) = &broker_stats.rtt {
            record_broker_gauge(
                "arroyo.producer.librdkafka.avg_rtt",
                rtt.avg as f64 / 1000.0,
                broker_id,
                producer_name,
            );
            record_broker_gauge(
                "arroyo.producer.librdkafka.p99_rtt",
                rtt.p99 as f64 / 1000.0,
                broker_id,
                producer_name,
            );
        }

        record_broker_gauge(
            "arroyo.producer.librdkafka.broker_txerrs",
            broker_stats.txerrs as f64,
            broker_id,
            producer_name,
        );
        record_broker_gauge(
            "arroyo.producer.librdkafka.broker_txretries",
            broker_stats.txretries as f64,
            broker_id,
            producer_name,
        );
        record_broker_gauge(
            "arroyo.producer.librdkafka.broker_tx",
            broker_stats.tx as f64,
            broker_id,
            producer_name,
        );
        record_broker_gauge(
            "arroyo.producer.librdkafka.broker_txbytes",
            broker_stats.txbytes as f64,
            broker_id,
            producer_name,
        );
    }

    record_producer_gauge(
        "arroyo.producer.librdkafka.message_count",
        stats.msg_cnt as f64,
        producer_name,
    );
    record_producer_gauge(
        "arroyo.producer.librdkafka.message_count_max",
        stats.msg_max as f64,
        producer_name,
    );
    record_producer_gauge(
        "arroyo.producer.librdkafka.message_size",
        stats.msg_size as f64,
        producer_name,
    );
    record_producer_gauge(
        "arroyo.producer.librdkafka.message_size_max",
        stats.msg_size_max as f64,
        producer_name,
    );
    record_producer_gauge(
        "arroyo.producer.librdkafka.reply_queue_size",
        stats.replyq as f64,
        producer_name,
    );
    record_producer_gauge(
        "arroyo.producer.librdkafka.txmsgs",
        stats.txmsgs as f64,
        producer_name,
    );
}

fn record_producer_gauge(name: &'static str, value: f64, producer_name: &str) {
    metrics::gauge!(name, "producer_name" => producer_name.to_owned()).set(value);
}

fn record_broker_gauge(name: &'static str, value: f64, broker_id: &str, producer_name: &str) {
    metrics::gauge!(
        name,
        "broker_id" => broker_id.to_owned(),
        "producer_name" => producer_name.to_owned()
    )
    .set(value);
}

fn record_broker_state(broker_id: &str, state: &str, producer_name: &str) {
    // Emitting every state on each callback resets the previous state's gauge instead of leaving
    // stale gauges behind.
    let state = if BROKER_STATES.contains(&state) {
        state
    } else {
        tracing::warn!(state, "unknown librdkafka broker state");
        "UNKNOWN"
    };

    for candidate in BROKER_STATES {
        metrics::gauge!(
            "arroyo.producer.librdkafka.broker_state",
            "broker_id" => broker_id.to_owned(),
            "producer_name" => producer_name.to_owned(),
            "state" => *candidate
        )
        .set(f64::from(*candidate == state));
    }
}

#[cfg(test)]
mod tests {
    use super::record;
    use metrics::{
        Counter, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };
    use rdkafka::statistics::{Broker, Statistics, Window};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct CapturingRecorder {
        gauges: Arc<Mutex<Vec<(Key, f64)>>>,
    }

    struct CapturingGauge {
        key: Key,
        gauges: Arc<Mutex<Vec<(Key, f64)>>>,
    }

    impl GaugeFn for CapturingGauge {
        fn increment(&self, value: f64) {
            self.set(value);
        }

        fn decrement(&self, value: f64) {
            self.set(-value);
        }

        fn set(&self, value: f64) {
            self.gauges
                .lock()
                .expect("capturing recorder lock poisoned")
                .push((self.key.clone(), value));
        }
    }

    impl Recorder for CapturingRecorder {
        fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
            Counter::noop()
        }

        fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
            Gauge::from_arc(Arc::new(CapturingGauge {
                key: key.clone(),
                gauges: self.gauges.clone(),
            }))
        }

        fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    fn gauge_value(metrics: &[(Key, f64)], name: &str, labels: &[(&str, &str)]) -> f64 {
        let matching = metrics
            .iter()
            .filter(|(key, _)| {
                key.name() == name
                    && labels.iter().all(|(name, value)| {
                        key.labels()
                            .any(|label| label.key() == *name && label.value() == *value)
                    })
            })
            .collect::<Vec<_>>();

        assert_eq!(matching.len(), 1, "expected one metric matching {name}");
        matching[0].1
    }

    fn create_test_statistics_with_all_metrics() -> Statistics {
        let mut brokers = HashMap::new();
        brokers.insert(
            "1".to_string(),
            Broker {
                state: "UP".to_string(),
                outbuf_cnt: 7,
                outbuf_msg_cnt: 8,
                waitresp_cnt: 9,
                connects: Some(10),
                disconnects: Some(11),
                txidle: 12_000,
                rxidle: 13_000,
                req_timeouts: 14,
                tx: 15,
                txbytes: 16,
                txerrs: 17,
                txretries: 18,
                int_latency: Some(Window {
                    p99: 1500,
                    avg: 750,
                    ..Default::default()
                }),
                outbuf_latency: Some(Window {
                    p99: 3750,
                    avg: 1250,
                    ..Default::default()
                }),
                rtt: Some(Window {
                    p99: 6500,
                    avg: 2750,
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        Statistics {
            brokers,
            replyq: 19,
            msg_cnt: 20,
            msg_max: 21,
            msg_size: 22,
            msg_size_max: 23,
            txmsgs: 24,
            ..Default::default()
        }
    }

    #[test]
    fn records_all_metrics() {
        let stats = create_test_statistics_with_all_metrics();
        let recorder = CapturingRecorder::default();

        metrics::with_local_recorder(&recorder, || record(&stats, "unknown"));

        let gauges = recorder
            .gauges
            .lock()
            .expect("capturing recorder lock poisoned");
        let producer_labels = [("producer_name", "unknown")];
        let broker_labels = [("broker_id", "1"), ("producer_name", "unknown")];

        for (name, expected) in [
            ("arroyo.producer.librdkafka.reply_queue_size", 19.0),
            ("arroyo.producer.librdkafka.message_count", 20.0),
            ("arroyo.producer.librdkafka.message_count_max", 21.0),
            ("arroyo.producer.librdkafka.message_size", 22.0),
            ("arroyo.producer.librdkafka.message_size_max", 23.0),
            ("arroyo.producer.librdkafka.txmsgs", 24.0),
        ] {
            assert_eq!(gauge_value(&gauges, name, &producer_labels), expected);
        }

        for (name, expected) in [
            ("arroyo.producer.librdkafka.broker_outbuf_requests", 7.0),
            ("arroyo.producer.librdkafka.broker_outbuf_messages", 8.0),
            ("arroyo.producer.librdkafka.broker_waitresp_requests", 9.0),
            ("arroyo.producer.librdkafka.broker_connects", 10.0),
            ("arroyo.producer.librdkafka.broker_disconnects", 11.0),
            ("arroyo.producer.librdkafka.broker_tx_idle", 12.0),
            ("arroyo.producer.librdkafka.broker_rx_idle", 13.0),
            ("arroyo.producer.librdkafka.broker_request_timeouts", 14.0),
            ("arroyo.producer.librdkafka.broker_tx", 15.0),
            ("arroyo.producer.librdkafka.broker_txbytes", 16.0),
            ("arroyo.producer.librdkafka.broker_txerrs", 17.0),
            ("arroyo.producer.librdkafka.broker_txretries", 18.0),
            ("arroyo.producer.librdkafka.avg_int_latency", 0.75),
            ("arroyo.producer.librdkafka.p99_int_latency", 1.5),
            ("arroyo.producer.librdkafka.avg_outbuf_latency", 1.25),
            ("arroyo.producer.librdkafka.p99_outbuf_latency", 3.75),
            ("arroyo.producer.librdkafka.avg_rtt", 2.75),
            ("arroyo.producer.librdkafka.p99_rtt", 6.5),
        ] {
            assert_eq!(gauge_value(&gauges, name, &broker_labels), expected);
        }

        assert_eq!(
            gauge_value(
                &gauges,
                "arroyo.producer.librdkafka.broker_state",
                &[
                    ("broker_id", "1"),
                    ("producer_name", "unknown"),
                    ("state", "UP"),
                ],
            ),
            1.0
        );
        assert_eq!(
            gauges
                .iter()
                .filter(|(key, _)| key.name() == "arroyo.producer.librdkafka.broker_state")
                .map(|(_, value)| value)
                .sum::<f64>(),
            1.0
        );
        assert_eq!(gauges.len(), 38);
    }

    #[test]
    fn handles_partial_metrics() {
        let stats = Statistics {
            brokers: HashMap::from([(
                "1".to_string(),
                Broker {
                    int_latency: Some(Window::default()),
                    outbuf_latency: Some(Window::default()),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };

        record(&stats, "unknown");
    }

    #[test]
    fn handles_no_brokers() {
        record(&Statistics::default(), "unknown");
    }

    #[test]
    fn handles_empty_broker_stats() {
        let stats = Statistics {
            brokers: HashMap::from([("1".to_string(), Broker::default())]),
            ..Default::default()
        };

        record(&stats, "unknown");
    }
}
