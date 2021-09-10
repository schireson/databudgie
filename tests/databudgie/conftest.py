import boto3
import pytest
from moto import mock_s3
from pytest_mock_resources import create_postgres_fixture, PostgresConfig

from tests.mockmodels.models import Base


@pytest.fixture(scope="session")
def pmr_postgres_config():
    return PostgresConfig(image="postgres:11-alpine")


pg = create_postgres_fixture(Base, session=True)


@pytest.fixture
def mf_config():
    return {"cleanup": False}


@pytest.fixture
def mf_session(pg):
    return pg


@pytest.fixture()
def s3_resource():
    with mock_s3():
        s3 = boto3.resource("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="sample-bucket")
        yield s3
