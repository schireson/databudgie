from unittest.mock import call, patch

from databudgie.manifest.manager import BackupManifest, RestoreManifest
from tests.mockmodels.models import DatabudgieManifest
from tests.test_backup import test_backup_all, test_backup_one
from tests.test_restore import test_restore_all, test_restore_one


def test_manifest_backup(pg, mf, s3_resource):
    manifest = BackupManifest(pg, DatabudgieManifest.__table__.name)
    test_backup_one(pg, mf, s3_resource, manifest=manifest)

    row = pg.query(DatabudgieManifest).first()

    assert row.transaction == 1
    assert row.table == "public.customer"
    assert row.file_path == "s3://sample-bucket/databudgie/test/public.customer/2021-04-26T09:00:00.csv"


def test_manifest_backup_resume_transaction(pg, mf, s3_resource, sample_config):
    manifest = BackupManifest(pg, DatabudgieManifest.__table__.name)
    test_backup_all(pg, s3_resource, manifest=manifest)

    with patch("databudgie.output.Console.trace") as console:
        test_backup_all(pg, s3_resource, manifest=manifest)
        assert console.call_count == 2
        console.assert_has_calls([call("Skipping public.customer..."), call("Skipping public.store...")])


def test_manifest_restore(pg, mf, s3_resource):
    manifest = RestoreManifest(pg, DatabudgieManifest.__table__.name)
    test_restore_one(pg, mf, s3_resource, manifest=manifest)

    row = pg.query(DatabudgieManifest).first()

    assert row.transaction == 1
    assert row.table == "public.product"
    assert row.file_path == "s3://sample-bucket/products/2021-04-26T09:00:00.csv"


def test_manifest_restore_resume_transaction(pg, s3_resource):
    manifest = RestoreManifest(pg, DatabudgieManifest.__table__.name)
    manifest.set_transaction_id(999)  # arbitary transaction id for better coverage

    test_restore_all(pg, s3_resource, manifest=manifest)

    with patch("databudgie.output.Console.trace") as console:
        test_restore_all(pg, s3_resource, manifest=manifest)
        console.assert_has_calls([call("Skipping public.store..."), call("Skipping public.product...")])


def test_manifest_subsequent_transaction(pg, mf):
    mf.manifest.backup(transaction=1, table="public.user", file_path="s3://sample-bucket/users/2021-04-26T09:00:00.csv")

    manifest = BackupManifest(pg, DatabudgieManifest.__table__.name)
    manifest.record("public.user", "s3://sample-bucket/users/2021-04-26T09:00:00.csv")

    rows = pg.query(DatabudgieManifest).all()
    assert len(rows) == 2
    assert rows[1].transaction == 2
