from typing import Sequence

from setuptools import find_packages, setup


def get_requirements() -> Sequence[str]:
    with open("requirements.txt") as fp:
        return [x.strip() for x in fp if not x.startswith("#")]


setup(
    name="sentry-arroyo",
    version="1.0.0",
    author="Sentry",
    author_email="oss@sentry.io",
    license="Apache-2.0",
    url="https://github.com/getsentry/arroyo",
    description="Arroyo is a Python library for working with streaming data.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["tests", "examples"]),
    package_data={"arroyo": ["py.typed"]},
    zip_safe=False,
    install_requires=get_requirements(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
