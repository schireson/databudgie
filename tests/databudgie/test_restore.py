import csv
import io
from typing import List

import faker
import pytest
from moto import mock_s3
from mypy_boto3_s3.service_resource import S3ServiceResource

from databudgie.restore import restore, restore_all
from databudgie.utils import wrap_buffer
from tests.mockmodels.models import Advertiser, Product

fake = faker.Faker()


def mock_s3_csv(s3_resource: S3ServiceResource, key: str, data: List[dict]):
    bucket = s3_resource.Bucket("sample-bucket")
    buffer = io.BytesIO()

    with wrap_buffer(buffer) as wrapper:
        writer: csv.DictWriter = csv.DictWriter(wrapper, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)

    bucket.put_object(Key=key, Body=buffer)


def test_restore_all(pg, sample_config, s3_resource):
    """Validate restore functionality for all tables in a config."""
    mock_advertiser = dict(id=1, name=fake.name())
    mock_product = dict(
        id=1,
        advertiser_id=1,
        external_id=str(fake.unique.pyint()),
        external_name=fake.name(),
        external_status="ACTIVE",
        active=True,
    )

    mock_s3_csv(s3_resource, "public.advertiser.csv", [mock_advertiser])
    mock_s3_csv(s3_resource, "public.product.csv", [mock_product])

    restore_all(pg, s3_resource, sample_config.restore.tables, strict=True)

    assert pg.query(Advertiser).count() == 1
    assert pg.query(Product).count() == 1


def test_restore_one(pg, mf, s3_resource):
    """Validate restore functionality for a single table."""

    mf.advertiser.new(name=fake.name())

    mock_products = [
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

    mock_s3_csv(s3_resource, "products.csv", mock_products)

    restore(pg, "product", s3_resource, "s3://sample-bucket/products.csv")

    products = pg.query(Product).all()
    assert len(products) == 2
    assert products[0].id == 1
    assert products[0].active is True
    assert products[0].external_id == mock_products[0]["external_id"]
    assert products[1].id == 2
    assert products[1].active is False
    assert products[1].external_id == mock_products[1]["external_id"]


def test_restore_overwrite_cascade(pg, mf, s3_resource):
    """Validate behavior for the cascading truncate option."""

    advertiser = mf.advertiser.new(name=fake.name())
    mf.product.new(id=1, advertiser_id=advertiser.id)
    mf.product.new(id=2, advertiser_id=advertiser.id)

    mock_product = dict(
        id=1,
        advertiser_id=advertiser.id,
        external_id=fake.unique.pyint(),
        external_name=fake.name(),
        external_status="ACTIVE",
        active=True,
    )

    mock_s3_csv(s3_resource, "products.csv", [mock_product])

    restore(pg, "product", s3_resource, "s3://sample-bucket/products.csv", truncate=True)

    advertisers = pg.query(Product).all()
    assert len(advertisers) == 1
