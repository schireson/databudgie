.PHONY: install format lint test build

VERSION=$(shell git rev-parse --short HEAD)

install:
	poetry install -E psycopg2-binary -E s3 -E sentry


format:
	ruff --fix src tests
	black src tests

lint:
	ruff src tests || exit 1
	mypy --namespace-packages src tests || exit 1
	black --check --diff src tests || exit 1

test:
	coverage run -a -m pytest src tests
	coverage report
	coverage xml

## Build
build-package:
	poetry build

build-docs:
	pip install -r docs/requirements.txt
	make -C docs html

build: build-package

publish: build
	poetry publish -u __token__ -p '${PYPI_PASSWORD}' --no-interaction
