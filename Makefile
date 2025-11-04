.PHONY: install format lint test build publish

VERSION=$(shell python -c 'from importlib import metadata; print(metadata.version("databudgie"))')

install:
	uv sync --extra postgres --extra s3


format:
	uv run ruff check --fix src tests
	uv run ruff format src tests

lint:
	uv run ruff check src tests || exit 1
	uv run mypy --namespace-packages src tests || exit 1

test:
	uv run coverage run -a -m pytest src tests
	uv run coverage report
	uv run coverage xml

## Build
build-package:
	uv build

build-docs:
	pip install -r docs/requirements.txt
	make -C docs html

build-image:
	docker build \
		-t schireson/databudgie \
		-t schireson/databudgie:latest \
		-t schireson/databudgie:$(VERSION) \
		.

build: build-package build-docs build-image

## Publish
publish-package: build-package
	uv publish --token '${PYPI_PASSWORD}'

publish-image: build-image
	docker push schireson/databudgie:latest
	docker push schireson/databudgie:$(VERSION)

publish: publish-package publish-image
