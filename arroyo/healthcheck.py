"""
Liveness check for consumers that use the ``Healthcheck`` strategy:

    python3 -m arroyo.healthcheck /tmp/health.txt

The strategy touches the file on every poll, at most once per second. This
check removes it, so the next check fails unless the consumer touches it again
in between. Imports only the standard library and ``arroyo`` itself, so it
starts fast and needs no shell in the image.
"""

import argparse
import os
import sys
from typing import Optional


def healthcheck(path: str) -> None:
    """
    Remove the health file, or exit 1 if the consumer has not touched it since
    the last check.
    """
    try:
        os.remove(path)
    except FileNotFoundError:
        sys.exit(
            f"{path} is missing: the consumer has not touched it since the last probe"
        )


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python3 -m arroyo.healthcheck",
        description="Remove the consumer health file, or exit 1 if it is missing.",
    )
    parser.add_argument("path", help="the file the Healthcheck strategy touches")
    args = parser.parse_args(argv)
    healthcheck(args.path)


if __name__ == "__main__":
    main()
