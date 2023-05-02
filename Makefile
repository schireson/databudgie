.PHONY: install format lint test build publish

VERSION=$(shell python -c 'from importlib import metadata; print(metadata.version("databudgie"))')

install:
	poetry install -E psycopg2-binary -E s3


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

build-image:
	docker build \
		-t databudgie \
		-t databudgie:latest \
		-t databudgie:$(VERSION) \
		.

build: build-package build-docs build-image

## Publish
publish-package: build-package
	poetry publish -u __token__ -p '${PYPI_PASSWORD}' --no-interaction

publish-image: build-image
	docker push databudgie:latest
	docker push databudgie:$(VERSION)

publish: publish-package publish-image
