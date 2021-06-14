.PHONY: install lint

install:
	pip install -e .

lint:
	mypy . --strict
