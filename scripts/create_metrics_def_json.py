import itertools
import json
import re
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Dict, List, Tuple, TypeVar

TElement = TypeVar("TElement")

METRICS_DEF_SRC_PATH = "arroyo/utils/metric_defs.py"
METRICS_DEF_JSON_PATH = (
    "arroyo/utils/metricDefs.json"  # must match name in `package_data`
)


def extract_literal_content_from_source(src_file: str) -> str:
    return re.search(r"\[(.|\n)*\]", src_file).group().lstrip("[\n").rstrip("\n]")  # type: ignore


def create_comment_metric_list(content_of_literal: str) -> List[str]:
    # split content on each metric - create a list of [comment_1,metric_1,...]
    return re.split(r"(\"arroyo\.[\w\.]+\")", content_of_literal)


def batched(iterable: Iterable[TElement], n: int) -> Iterator[Tuple[TElement, ...]]:
    # taken from https://docs.python.org/3/library/itertools.html#itertools.batched
    #
    # because CI runs on 3.8 and itertools.batched ships with 3.12
    #
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    iterator = iter(iterable)
    while batch := tuple(itertools.islice(iterator, n)):
        yield batch


def parse_metric_name(metric_name_raw: str) -> str:
    return metric_name_raw.replace('"', "")


def parse_description_comment(comment: str) -> Tuple[str, str]:
    # a metric description is a single or multi-line comment where the metric type precedes
    # on the first line e.g. <MetricType>: <Description>
    type_, description = (
        re.sub(r"(#\s|#)", "", comment)
        .lstrip(",\n")
        .rstrip("\n")
        .split(": ", maxsplit=1)
    )
    return type_, description


def create_machine_readable_structure(
    comment_metric_list: List[str],
) -> Dict[str, Dict[str, str]]:
    metrics = {}
    for full_description, metric in batched(
        filter(lambda x: x != ",", comment_metric_list), 2
    ):
        name = parse_metric_name(metric)
        type_, description = parse_description_comment(full_description)
        metrics[name] = {"name": name, "type": type_, "description": description}

    return metrics


def main() -> None:
    src_file = Path(METRICS_DEF_SRC_PATH).read_text()

    # remove indentation
    src_file = src_file.replace(" " * 4, "")

    content_of_literal = extract_literal_content_from_source(src_file)

    comment_metric_list = create_comment_metric_list(content_of_literal)

    metrics = create_machine_readable_structure(comment_metric_list)

    with open(Path(METRICS_DEF_JSON_PATH), mode="w") as f:
        json.dump(metrics, f)


if __name__ == "__main__":
    main()
