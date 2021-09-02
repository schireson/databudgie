.PHONY: init install format lint test docker-login-cmd build push pull enter

AWS_PROFILE ?= media-activation-prod
VERSION=$(shell git rev-parse --short HEAD)

init:
	pyenv install -s 3.7.10
	pyenv virtualenv 3.7.10 ma-databudgie-py37

install:
	poetry install -E psycopg2-binary


format:
	isort --recursive --quiet src tests
	black src tests

lint:
	isort --recursive --quiet --check --diff src tests || exit 1
	flake8 --max-line-length=200 src tests || exit 1
	pydocstyle src tests || exit 1
	mypy --namespace-packages src tests || exit 1
	bandit -r src || exit 1
	black --check --diff src tests || exit 1

test:
	coverage run -a -m py.test src tests
	coverage report
	coverage xml

docker-login-cmd:
	eval $$(aws --profile=$(AWS_PROFILE) ecr get-login --no-include-email --region us-east-1)

build:
	docker build . \
		-t databudgie \
		-t 233478501758.dkr.ecr.us-east-1.amazonaws.com/databudgie \
		--build-arg VERSION=$(VERSION)

push:
	docker push 233478501758.dkr.ecr.us-east-1.amazonaws.com/databudgie

pull:
	docker pull 233478501758.dkr.ecr.us-east-1.amazonaws.com/databudgie

enter:
	docker run --env-file .env -v ${PWD}:/app -it 233478501758.dkr.ecr.us-east-1.amazonaws.com/databudgie bash
