.PHONY: install lint setup-git tests docs

setup-git:
	pip install pre-commit==2.13.0
	pre-commit install --install-hooks

install:
	pip install -e .
	pip install -r requirements-test.txt

lint:
	mypy . --strict

tests:
	pytest -vv

docs:
	pip install -U -r ./docs-requirements.txt
	sphinx-build -W -b html docs/source docs/build
