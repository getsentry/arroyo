from typing import Any, Mapping, Optional

import pytest

from arroyo.backends.kafka.configuration import (
    DEFAULT_QUEUED_MAX_MESSAGE_KBYTES,
    DEFAULT_QUEUED_MIN_MESSAGES,
    build_kafka_consumer_configuration,
)

PREFETCH_SETTINGS = [
    pytest.param(
        "queued.max.messages.kbytes",
        "queued_max_messages_kbytes",
        DEFAULT_QUEUED_MAX_MESSAGE_KBYTES,
        id="queued.max.messages.kbytes",
    ),
    pytest.param(
        "queued.min.messages",
        "queued_min_messages",
        DEFAULT_QUEUED_MIN_MESSAGES,
        id="queued.min.messages",
    ),
]


def _build(
    default_config: Mapping[str, Any],
    override_params: Optional[Mapping[str, Any]] = None,
    arguments: Optional[Mapping[str, Any]] = None,
) -> Mapping[str, Any]:
    return build_kafka_consumer_configuration(
        default_config,
        group_id="test-group",
        bootstrap_servers=["localhost:9092"],
        override_params=override_params,
        **(arguments or {}),
    )


@pytest.mark.parametrize("key, argument, default", PREFETCH_SETTINGS)
def test_prefetch_default(key: str, argument: str, default: int) -> None:
    config = _build({})
    assert config[key] == default


@pytest.mark.parametrize("key, argument, default", PREFETCH_SETTINGS)
def test_prefetch_from_default_config(key: str, argument: str, default: int) -> None:
    config = _build({key: 1234})
    assert config[key] == 1234

    # The caller's type is kept: librdkafka also accepts strings.
    config = _build({key: "1234"})
    assert config[key] == "1234"


@pytest.mark.parametrize("key, argument, default", PREFETCH_SETTINGS)
def test_prefetch_from_override_params(key: str, argument: str, default: int) -> None:
    config = _build({}, override_params={key: 2345})
    assert config[key] == 2345

    config = _build({key: 1234}, override_params={key: 2345})
    assert config[key] == 2345


@pytest.mark.parametrize("key, argument, default", PREFETCH_SETTINGS)
def test_prefetch_none_is_unset(key: str, argument: str, default: int) -> None:
    # None is never passed on to librdkafka.
    config = _build({key: None})
    assert config[key] == default

    config = _build({}, override_params={key: None})
    assert config[key] == default

    # override_params replaces default_config in the merge, so a None there
    # selects arroyo's default rather than the default_config value.
    config = _build({key: 1234}, override_params={key: None})
    assert config[key] == default


@pytest.mark.parametrize("key, argument, default", PREFETCH_SETTINGS)
def test_prefetch_argument_wins(key: str, argument: str, default: int) -> None:
    config = _build({}, arguments={argument: 3456})
    assert config[key] == 3456

    config = _build(
        {key: 1234}, override_params={key: 2345}, arguments={argument: 3456}
    )
    assert config[key] == 3456


def test_prefetch_settings_are_independent() -> None:
    config = _build(
        {"queued.max.messages.kbytes": 1234},
        arguments={"queued_min_messages": 10},
    )
    assert config["queued.max.messages.kbytes"] == 1234
    assert config["queued.min.messages"] == 10

    config = _build({"queued.min.messages": 10})
    assert config["queued.max.messages.kbytes"] == DEFAULT_QUEUED_MAX_MESSAGE_KBYTES
    assert config["queued.min.messages"] == 10


@pytest.mark.parametrize("source", ["default_config", "override_params"])
def test_arroyo_owned_settings_still_overwritten(source: str) -> None:
    # Only the prefetch settings are taken from the caller's config. The
    # settings arroyo relies on still overwrite what the caller gave.
    caller_config = {
        "enable.auto.commit": True,
        "enable.auto.offset.store": True,
        "group.id": "other-group",
        "queued.min.messages": 10,
    }
    if source == "default_config":
        config = _build(caller_config)
    else:
        config = _build({}, override_params=caller_config)

    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["group.id"] == "test-group"
    assert config["queued.min.messages"] == 10
