import click
import pytest
from pytest_mock_resources import create_postgres_fixture
from sqlalchemy import text

from databudgie.cli.base import _create_postgres_session
from databudgie.config import BackupConfig, ConfigStack

pg_engine = create_postgres_fixture()


def test_create_postgres_session_str(pg_engine):
    url = pg_engine.pmr_credentials.as_sqlalchemy_url()

    try:
        url_str = url.render_as_string(hide_password=False)
    except AttributeError:
        url_str = str(url)

    assert url_str.startswith("postgresql+psycopg")
    config = BackupConfig.from_stack(ConfigStack({"url": url_str}))

    session = _create_postgres_session(config)
    session.execute(text("select 1"))


def test_create_postgres_session_url_components(pg_engine):
    url_parts = pg_engine.pmr_credentials.as_sqlalchemy_url_kwargs()
    assert isinstance(url_parts, dict)

    config = BackupConfig.from_stack(ConfigStack({"url": url_parts}))
    session = _create_postgres_session(config)
    session.execute(text("select 1"))


def test_connection_selection(pg_engine):
    url = pg_engine.pmr_credentials.as_sqlalchemy_url()

    try:
        url_str = url.render_as_string(hide_password=False)
    except AttributeError:
        url_str = str(url)

    assert url_str.startswith("postgresql+psycopg")
    config = BackupConfig.from_stack(ConfigStack({"connections": {"foo": url_str}, "connection": "foo"}))

    session = _create_postgres_session(config)
    session.execute(text("select 1"))


def test_missing_connection():
    config = BackupConfig.from_stack(ConfigStack({"connections": {}, "connection": "foo"}))

    with pytest.raises(click.UsageError):
        _create_postgres_session(config)
