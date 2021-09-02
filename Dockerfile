FROM python:3.9.6-slim-buster

WORKDIR /app

ENV PATH="/root/.poetry/bin:${PATH}" VERSION=${VERSION}

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install curl build-essential python3-dev libpq-dev -y \
    && curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | POETRY_VERSION=1.1.8 python \
    && poetry config virtualenvs.create false

COPY poetry.lock pyproject.toml ./

RUN poetry install -E psycopg2 --no-root

COPY . .

RUN poetry install

CMD [ "databudgie" ]
