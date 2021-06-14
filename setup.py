from typing import Sequence

from setuptools import find_packages, setup

VERSION = "0.0.1"


def get_requirements() -> Sequence[str]:
    with open(u"requirements.txt") as fp:
        return [x.strip() for x in fp.read().split("\n") if not x.startswith("#")]


setup(
    name="arroyo",
    packages=find_packages(exclude=["tests"]),
    zip_safe=False,
    install_requires=get_requirements(),
)
