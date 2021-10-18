import boto3
import pytest
from configly import Config
from freezegun import freeze_time
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


@pytest.fixture(autouse=True)
def fixed_time():
    with freeze_time("2021-04-26 09:00:00"):
        yield


@pytest.fixture()
def sample_config():
    yield Config(
        {
            "backup": {
                "tables": {
                    "public.store": {
                        "location": "s3://sample-bucket/databudgie/test/public.store",
                        "query": "select * from public.store",
                    },
                    "public.customer": {
                        "location": "s3://sample-bucket/databudgie/test/public.customer",
                        "query": "select * from public.customer",
                    },
                }
            },
            "restore": {
                "tables": {
                    "public.store": {"location": "s3://sample-bucket/public.store", "strategy": "use_latest_filename"},
                    "public.product": {
                        "location": "s3://sample-bucket/public.product",
                        "strategy": "use_latest_metadata",
                    },
                }
            },
        }
    )
