import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

from arroyo.healthcheck import main
from arroyo.processing.strategies.healthcheck import Healthcheck


def test_fails_when_consumer_has_not_touched_file_since_last_check(
    tmp_path: Path,
) -> None:
    path = tmp_path / "health.txt"
    Healthcheck(str(path), mock.Mock()).poll()

    main([str(path)])
    assert not path.exists()

    with pytest.raises(SystemExit) as exc:
        main([str(path)])
    assert exc.value.code == (
        f"{path} is missing: the consumer has not touched it since the last probe"
    )


def test_import_stays_light() -> None:
    # The check runs as a liveness probe every few seconds, so it must not pull
    # in the Kafka client or the processing machinery.
    code = (
        "import sys, arroyo.healthcheck; "
        "print(sorted(m for m in ('confluent_kafka', 'arroyo.processing') if m in sys.modules))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "[]"
