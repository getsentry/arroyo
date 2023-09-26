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
        auto_offset_reset = "error"

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
