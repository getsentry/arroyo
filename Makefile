.PHONY: install

install:
	pip install -e .

lint:
	mypy . --strict
