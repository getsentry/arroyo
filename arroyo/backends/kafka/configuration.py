import copy
import json
import logging
from typing import Any, Dict, Mapping, Optional, Sequence

from arroyo.utils.logging import pylog_to_syslog_level
from arroyo.utils.metrics import get_metrics

logger = logging.getLogger(__name__)

KafkaBrokerConfig = Dict[str, Any]

STATS_COLLECTION_FREQ_MS = 1000


DEFAULT_QUEUED_MAX_MESSAGE_KBYTES = 50000
DEFAULT_QUEUED_MIN_MESSAGES = 10000
DEFAULT_PARTITIONER = "consistent"
DEFAULT_MAX_MESSAGE_BYTES = 50000000  # 50MB, default is 1MB


def build_kafka_configuration(
    default_config: Mapping[str, Any],
    bootstrap_servers: Optional[Sequence[str]] = None,
    override_params: Optional[Mapping[str, Any]] = None,
) -> KafkaBrokerConfig:
    default_bootstrap_servers = None
    broker_config = copy.deepcopy(default_config)
    assert isinstance(broker_config, dict)
    bootstrap_servers = (
        ",".join(bootstrap_servers) if bootstrap_servers else default_bootstrap_servers
    )
    if bootstrap_servers:
        broker_config["bootstrap.servers"] = bootstrap_servers
    broker_config = {k: v for k, v in broker_config.items() if v is not None}

    broker_config["log_level"] = pylog_to_syslog_level(logger.getEffectiveLevel())

    if override_params:
        broker_config.update(override_params)

    return broker_config


def stats_callback(stats_json: str) -> None:
    stats = json.loads(stats_json)
    get_metrics().gauge(
        "arroyo.consumer.librdkafka.total_queue_size", stats.get("replyq", 0)
    )


def producer_stats_callback(stats_json: str) -> None:
    stats = json.loads(stats_json)
    metrics = get_metrics()

    # Extract broker-level int_latency metrics
    brokers = stats.get("brokers", {})
    for broker_id, broker_stats in brokers.items():
        int_latency = broker_stats.get("int_latency", {})
        if int_latency:
            p99_latency_ms = int_latency.get("p99", 0) / 1000.0
            metrics.timing(
                "arroyo.producer.librdkafka.p99_int_latency",
                p99_latency_ms,
                tags={"broker_id": str(broker_id)},
            )
            avg_latency_ms = int_latency.get("avg", 0) / 1000.0
            metrics.timing(
                "arroyo.producer.librdkafka.avg_int_latency",
                avg_latency_ms,
                tags={"broker_id": str(broker_id)},
            )

        outbuf_latency = broker_stats.get("outbuf_latency", {})
        if outbuf_latency:
            p99_latency_ms = outbuf_latency.get("p99", 0) / 1000.0
            metrics.timing(
                "arroyo.producer.librdkafka.p99_outbuf_latency",
                p99_latency_ms,
                tags={"broker_id": str(broker_id)},
            )
            avg_latency_ms = outbuf_latency.get("avg", 0) / 1000.0
            metrics.timing(
                "arroyo.producer.librdkafka.avg_outbuf_latency",
                avg_latency_ms,
                tags={"broker_id": str(broker_id)},
            )

        rtt = broker_stats.get("rtt", {})
        if rtt:
            p99_rtt_ms = rtt.get("p99", 0) / 1000.0
            metrics.timing(
                "arroyo.producer.librdkafka.p99_rtt",
                p99_rtt_ms,
                tags={"broker_id": str(broker_id)},
            )
            avg_rtt_ms = rtt.get("avg", 0) / 1000.0
            metrics.timing(
                "arroyo.producer.librdkafka.avg_rtt",
                avg_rtt_ms,
                tags={"broker_id": str(broker_id)},
            )


def build_kafka_producer_configuration(
    default_config: Mapping[str, Any],
    bootstrap_servers: Optional[Sequence[str]] = None,
    override_params: Optional[Mapping[str, Any]] = None,
) -> KafkaBrokerConfig:
    broker_config = build_kafka_configuration(
        default_config, bootstrap_servers, override_params
    )

    broker_config.update(
        {
            "statistics.interval.ms": STATS_COLLECTION_FREQ_MS,
            "stats_cb": producer_stats_callback,
        }
    )
    return broker_config


def build_kafka_consumer_configuration(
    default_config: Mapping[str, Any],
    group_id: str,
    auto_offset_reset: Optional[str] = None,
    queued_max_messages_kbytes: Optional[int] = None,
    queued_min_messages: Optional[int] = None,
    bootstrap_servers: Optional[Sequence[str]] = None,
    override_params: Optional[Mapping[str, Any]] = None,
    strict_offset_reset: Optional[bool] = None,
) -> KafkaBrokerConfig:

    if auto_offset_reset is None:
        auto_offset_reset = "earliest"

    if queued_max_messages_kbytes is None:
        queued_max_messages_kbytes = DEFAULT_QUEUED_MAX_MESSAGE_KBYTES

    if queued_min_messages is None:
        queued_min_messages = DEFAULT_QUEUED_MIN_MESSAGES

    broker_config = build_kafka_configuration(
        default_config, bootstrap_servers, override_params
    )

    broker_config.update(
        {
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            # this is an arroyo specific flag that only affects the consumer.
            "arroyo.strict.offset.reset": strict_offset_reset,
            # overridden to reduce memory usage when there's a large backlog
            "queued.max.messages.kbytes": queued_max_messages_kbytes,
            "queued.min.messages": queued_min_messages,
            "enable.partition.eof": False,
            "statistics.interval.ms": STATS_COLLECTION_FREQ_MS,
            "stats_cb": stats_callback,
        }
    )
    return broker_config
