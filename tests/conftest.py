import logging

import boto3
import pytest
from freezegun import freeze_time
from moto import mock_aws
from pytest_mock_resources import create_postgres_fixture, PostgresConfig

from databudgie.config import RootConfig
from tests.mockmodels.models import Base

logging.basicConfig(level="INFO")


@pytest.fixture(scope="session")
def pmr_postgres_config():
    return PostgresConfig(image="postgres:11-alpine")


pg = create_postgres_fixture(Base, session=True, createdb_template="template0")


@pytest.fixture
def mf_config():
    return {"cleanup": False}


@pytest.fixture
def mf_session(pg):
    return pg


@pytest.fixture
def s3_config():
    yield {
        "s3": {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "foo",
            "region": "foo",
        }
    }


@pytest.fixture()
def s3_resource():
    with mock_aws():
        s3 = boto3.resource("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="sample-bucket")
        yield s3


@pytest.fixture(autouse=True)
def fixed_time():
    with freeze_time("2021-04-26 09:00:00"):
        yield


@pytest.fixture()
def sample_config():
    yield RootConfig.from_dict(
        {
            "s3": {},
            "root_location": "s3://sample-bucket",
            "backup": {
                "tables": {
                    "public.store": {
                        "location": "databudgie/test/public.store",
                        "query": "select * from public.store",
                    },
                    "public.customer": {
                        "location": "databudgie/test/public.customer",
                        "query": "select * from public.customer",
                    },
                }
            },
            "restore": {
                "tables": {
                    "public.store": {
                        "location": "public.store",
                        "strategy": "use_latest_filename",
                    },
                    "public.product": {
                        "location": "public.product",
                        "strategy": "use_latest_metadata",
                    },
                }
            },
        }
    )
