from unittest.mock import call, patch

import faker

from databudgie.backup import backup_all
from databudgie.config import RootConfig
from databudgie.manifest.manager import BackupManifest, RestoreManifest
from databudgie.restore import restore_all
from databudgie.storage import StorageBackend
from tests.mockmodels.models import DatabudgieManifest
from tests.test_backup import (
    _validate_backup_contents,
)
from tests.utils import get_file_buffer, mock_s3_csv, s3_config

fake = faker.Faker()


def test_manifest_backup(pg, mf, s3_resource):
    customer = mf.customer.new(external_id="cid_123")

    config = RootConfig.from_dict(
        {"tables": {"public.customer": {"location": "s3://sample-bucket/databudgie/test/public.customer"}}, **s3_config}
    )

    manifest = BackupManifest(pg, DatabudgieManifest.__table__.name)
    storage = StorageBackend.from_config(config.backup, manifest)
    backup_all(pg, config.backup, storage)

    _validate_backup_contents(
        get_file_buffer("s3://sample/databudgie/test/public.customer/2021-04-26T09:00:00.csv", s3_resource), [customer]
    )

    row = pg.query(DatabudgieManifest).first()

    assert row.transaction == 1
    assert row.table == "public.customer"
    assert row.file_path == "s3://sample-bucket/databudgie/test/public.customer/2021-04-26T09:00:00.csv"


def test_manifest_backup_resume_transaction(pg, s3_resource):
    config = RootConfig.from_dict(
        {"tables": {"public.customer": {"location": "s3://sample-bucket/databudgie/test/public.customer"}}, **s3_config}
    )
    manifest = BackupManifest(pg, DatabudgieManifest.__table__.name)
    storage = StorageBackend.from_config(config.backup, manifest)

    backup_all(pg, config.backup, storage)

    with patch("databudgie.output.Console.trace") as console:
        backup_all(pg, config.backup, storage)
        assert console.call_count == 1
        console.assert_has_calls([call("Skipping public.customer...")])


def test_manifest_restore(pg, s3_resource):
    config = RootConfig.from_dict(
        {
            **s3_config,
            "strict": True,
            "restore": {
                "tables": {
                    "public.store": {"location": "s3://sample-bucket/public.store", "strategy": "use_latest_filename"},
                    "public.product": {
                        "location": "s3://sample-bucket/public.product",
                        "strategy": "use_latest_metadata",
                    },
                },
            },
        }
    )
    manifest = BackupManifest(pg, DatabudgieManifest.__table__.name)
    storage = StorageBackend.from_config(config.restore, manifest)

    mock_store = {"id": 1, "name": fake.name()}
    mock_product = {
        "id": 1,
        "store_id": 1,
        "external_id": str(fake.unique.pyint()),
        "external_name": fake.name(),
        "external_status": "ACTIVE",
        "active": True,
    }

    mock_s3_csv(s3_resource, "public.store/2021-04-26T09:00:00.csv", [mock_store])
    mock_s3_csv(s3_resource, "public.product/2021-04-26T09:00:00.csv", [mock_product])

    restore_all(pg, config.restore, storage)

    row = pg.query(DatabudgieManifest).order_by(DatabudgieManifest.table).first()

    assert row.transaction == 1
    assert row.table == "public.product"
    assert row.file_path == "s3://sample-bucket/public.product/2021-04-26T09:00:00.csv"


def test_manifest_restore_resume_transaction(pg, mf, s3_resource):
    store = mf.store.new(name=fake.name())

    mock_products = [
        {
            "id": 1,
            "store_id": store.id,
            "external_id": str(fake.unique.pyint()),
            "external_name": fake.name(),
            "external_status": "ACTIVE",
            "active": True,
        },
        {
            "id": 2,
            "store_id": store.id,
            "external_id": str(fake.unique.pyint()),
            "external_name": fake.name(),
            "external_status": None,
            "active": False,
        },
    ]

    mock_s3_csv(s3_resource, "products/2021-04-26T09:00:00.csv", mock_products)

    config = RootConfig.from_dict({**s3_config, "tables": {"product": {"location": "s3://sample-bucket/products"}}})

    manifest = RestoreManifest(pg, DatabudgieManifest.__table__.name)
    manifest.set_transaction_id(999)  # arbitary transaction id for better coverage

    storage = StorageBackend.from_config(config.restore, manifest)
    restore_all(pg, config.restore, storage)

    with patch("databudgie.output.Console.trace") as console:
        restore_all(pg, config.restore, storage)
        console.assert_has_calls([call("Skipping public.product...")])


def test_manifest_subsequent_transaction(pg, mf):
    mf.manifest.backup(transaction=1, table="public.user", file_path="s3://sample-bucket/users/2021-04-26T09:00:00.csv")

    manifest = BackupManifest(pg, DatabudgieManifest.__table__.name)
    manifest.record("public.user", "s3://sample-bucket/users/2021-04-26T09:00:00.csv")

    rows = pg.query(DatabudgieManifest).all()
    assert len(rows) == 2
    assert rows[1].transaction == 2
