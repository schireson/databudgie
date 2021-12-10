import csv
import io
from typing import List

import faker
from configly import Config
from mypy_boto3_s3.service_resource import S3ServiceResource

from databudgie.adapter.base import Adapter
from databudgie.etl.base import TableOp
from databudgie.etl.restore import restore, restore_all
from databudgie.utils import wrap_buffer
from tests.mockmodels.models import Product, Store

fake = faker.Faker()


def mock_s3_csv(s3_resource: S3ServiceResource, key: str, data: List[dict]):
    bucket = s3_resource.Bucket("sample-bucket")
    buffer = io.BytesIO()

    with wrap_buffer(buffer) as wrapper:
        writer: csv.DictWriter = csv.DictWriter(wrapper, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)

    bucket.put_object(Key=key, Body=buffer)


def test_restore_all(pg, sample_config, s3_resource, **extras):
    """Validate restore functionality for all tables in a config."""
    mock_store = dict(id=1, name=fake.name())
    mock_product = dict(
        id=1,
        store_id=1,
        external_id=str(fake.unique.pyint()),
        external_name=fake.name(),
        external_status="ACTIVE",
        active=True,
    )

    mock_s3_csv(s3_resource, "public.store/2021-04-26T09:00:00.csv", [mock_store])
    mock_s3_csv(s3_resource, "public.product/2021-04-26T09:00:00.csv", [mock_product])

    restore_all(pg, s3_resource, sample_config, strict=True, **extras)

    assert pg.query(Store).count() == 1
    assert pg.query(Product).count() == 1


def test_restore_one(pg, mf, s3_resource, **extras):
    """Validate restore functionality for a single table."""

    store = mf.store.new(name=fake.name())

    mock_products = [
        dict(
            id=1,
            store_id=store.id,
            external_id=str(fake.unique.pyint()),
            external_name=fake.name(),
            external_status="ACTIVE",
            active=True,
        ),
        dict(
            id=2,
            store_id=store.id,
            external_id=str(fake.unique.pyint()),
            external_name=fake.name(),
            external_status=None,
            active=False,
        ),
    ]

    mock_s3_csv(s3_resource, "products/2021-04-26T09:00:00.csv", mock_products)

    restore(
        pg,
        s3_resource,
        adapter=Adapter.get_adapter(pg),
        config=None,
        table_op=TableOp("product", dict(location="s3://sample-bucket/products")),
        **extras
    )

    products = pg.query(Product).all()
    assert len(products) == 2
    assert products[0].id == 1
    assert products[0].active is True
    assert products[0].external_id == mock_products[0]["external_id"]
    assert products[1].id == 2
    assert products[1].active is False
    assert products[1].external_id == mock_products[1]["external_id"]


def test_restore_all_overwrite_cascade(pg, mf, s3_resource):
    """Validate behavior for the cascading truncate option."""

    store = mf.store.new(name=fake.name())
    mf.product.new(store=store)
    mf.product.new(store=store)

    mock_product = dict(
        id=1,
        store_id=store.id,
        external_id=fake.unique.pyint(),
        external_name=fake.name(),
        external_status="ACTIVE",
        active=True,
    )

    mock_s3_csv(s3_resource, "products/2021-04-26T09:00:00.csv", [mock_product])

    restore_all(
        pg,
        s3_resource,
        config=Config(
            {"restore": {"tables": {"product": {"truncate": True, "location": "s3://sample-bucket/products"}}}}
        ),
    )

    stores = pg.query(Product).all()
    assert len(stores) == 1


def test_restore_glob(pg, mf, s3_resource):
    """Validate restore composes with glob table specifications."""

    # Prove we truncate the rows
    store = mf.store.new(name=fake.name())
    mf.product.new(store=store)
    mf.product.new(store=store)

    mock_s3_csv(
        s3_resource,
        "public.store/2021-04-26T09:00:00.csv",
        [
            dict(id=1, name=fake.name()),
            dict(id=2, name=fake.name()),
        ],
    )
    mock_s3_csv(
        s3_resource,
        "public.product/2021-04-26T09:00:00.csv",
        [
            dict(
                id=1,
                store_id=1,
                external_id=fake.unique.pyint(),
                external_name=fake.name(),
                external_status="ACTIVE",
                active=True,
            ),
        ],
    )

    config = Config({"restore": {"tables": {"public.*": {"location": "s3://sample-bucket/{table}", "truncate": True}}}})

    restore_all(pg, s3_resource, config)

    stores = pg.query(Store).all()
    assert len(stores) == 2

    stores = pg.query(Product).all()
    assert len(stores) == 1
