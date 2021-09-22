import csv
import io
from typing import List

import faker
import pytest
from moto import mock_s3
from mypy_boto3_s3.service_resource import Bucket

from databudgie.restore import restore, restore_all
from databudgie.utils import wrap_buffer
from tests.mockmodels.models import Advertiser, LineItem

fake = faker.Faker()


@pytest.fixture
def mock_bucket(s3_resource):
    with mock_s3():
        bucket: Bucket = s3_resource.create_bucket(Bucket="sample-bucket")
        yield bucket


def mock_s3_csv(bucket: Bucket, key: str, data: List[dict]):
    buffer = io.BytesIO()

    with wrap_buffer(buffer) as wrapper:
        writer: csv.DictWriter = csv.DictWriter(wrapper, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)

    bucket.put_object(Key=key, Body=buffer)


def test_restore_all(pg, mock_bucket, s3_resource, sample_config):
    """Validate restore functionality for all tables in a config."""
    mock_advertiser = dict(id=1, name=fake.name())
    mock_line_item = dict(
        id=1,
        advertiser_id=1,
        external_id=str(fake.unique.pyint()),
        external_name=fake.name(),
        external_status="ACTIVE",
        active=True,
    )

    mock_s3_csv(mock_bucket, "public.advertiser.csv", [mock_advertiser])
    mock_s3_csv(mock_bucket, "public.line_item.csv", [mock_line_item])

    restore_all(pg, s3_resource, sample_config.restore.tables, strict=True)

    assert pg.query(Advertiser).count() == 1
    assert pg.query(LineItem).count() == 1


def test_restore_one(pg, mf, s3_resource, mock_bucket):
    """Validate restore functionality for a single table."""

    mf.advertiser.new(name=fake.name())

    mock_line_items = [
        dict(
            id=1,
            advertiser_id=1,
            external_id=str(fake.unique.pyint()),
            external_name=fake.name(),
            external_status="ACTIVE",
            active=True,
        ),
        dict(
            id=2,
            advertiser_id=1,
            external_id=str(fake.unique.pyint()),
            external_name=fake.name(),
            external_status=None,
            active=False,
        ),
    ]
    mock_s3_csv(mock_bucket, "line_items.csv", mock_line_items)

    restore(pg, "line_item", s3_resource, "s3://sample-bucket/line_items.csv")

    line_items = pg.query(LineItem).all()
    assert len(line_items) == 2
    assert line_items[0].id == 1
    assert line_items[0].active is True
    assert line_items[0].external_id == mock_line_items[0]["external_id"]
    assert line_items[1].id == 2
    assert line_items[1].active is False
    assert line_items[1].external_id == mock_line_items[1]["external_id"]


def test_restore_overwrite_cascade(pg, mf, s3_resource, mock_bucket):
    """Validate behavior for the cascading truncate option."""

    advertiser = mf.advertiser.new(name=fake.name())
    mf.line_item.new(id=1, advertiser_id=advertiser.id)
    mf.line_item.new(id=2, advertiser_id=advertiser.id)

    mock_line_item = dict(
        id=1,
        advertiser_id=advertiser.id,
        external_id=fake.unique.pyint(),
        external_name=fake.name(),
        external_status="ACTIVE",
        active=True,
    )

    mock_s3_csv(mock_bucket, "line_items.csv", [mock_line_item])

    restore(pg, "line_item", s3_resource, "s3://sample-bucket/line_items.csv", truncate=True)

    advertisers = pg.query(LineItem).all()
    assert len(advertisers) == 1
