import argparse
import os
import sys
from typing import Optional


def healthcheck(path: str) -> None:
    """
    Remove the file the Healthcheck strategy touches, so the next check fails
    unless the consumer touches it again. Runs as the consumer's liveness probe.
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
