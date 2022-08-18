from pytest_mock_resources import create_postgres_fixture
from sqlalchemy import text

from databudgie.cli.base import _create_postgres_session

pg_engine = create_postgres_fixture()


def test_create_postgres_session_str(pg_engine):
    url = pg_engine.pmr_credentials.as_sqlalchemy_url()
    url_str = str(url)
    assert url_str.startswith("postgresql+psycopg2://")

    session = _create_postgres_session(url_str)
    session.execute(text("select 1"))


def test_create_postgres_session_url_components(pg_engine):
    url_parts = pg_engine.pmr_credentials.as_sqlalchemy_url_kwargs()
    assert isinstance(url_parts, dict)

    session = _create_postgres_session(url_parts)
    session.execute(text("select 1"))
