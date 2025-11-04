FROM python:3.9 as base

RUN python3.9 -m venv /opt/venv

ENV PATH="/opt/venv/bin:/root/.local/bin:${PATH}" \
    VIRTUAL_ENV="/opt/venv"

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install --no-install-recommends -y \
    curl build-essential libpq-dev postgresql-client \
    && curl -sSL https://install.python-poetry.org/ | POETRY_VERSION=1.2.2 python3.9 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY poetry.lock pyproject.toml README.md ./
COPY src src

RUN poetry build
RUN (export version=$(find dist -name '*.whl'); \
    pip install "${version}[s3,postgres]")

FROM python:3.9

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="/home/app/src:$PYTHONPATH" \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

COPY --from=base /opt/venv /opt/venv

ENTRYPOINT [ "databudgie" ]
CMD [ "config"  ]
