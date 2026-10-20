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
