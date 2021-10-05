from unittest.mock import call, patch

from databudgie.manifest.manager import BackupManifest, RestoreManifest
from tests.databudgie.test_backup import test_backup_all, test_backup_one
from tests.databudgie.test_restore import test_restore_all, test_restore_one
from tests.mockmodels.models import DatabudgieManifest


def test_manifest_backup(pg, mf, s3_resource):
    manifest = BackupManifest(pg, DatabudgieManifest.__tablename__)
    test_backup_one(pg, mf, s3_resource, manifest=manifest)

    row = pg.query(DatabudgieManifest).first()

    assert row.transaction == 1
    assert row.table == "public.customer"
    assert row.file_path == "s3://sample-bucket/databudgie/test/public.customer.csv"


def test_manifest_backup_resume_transaction(pg, mf, s3_resource, sample_config):
    manifest = BackupManifest(pg, DatabudgieManifest.__tablename__)
    test_backup_all(pg, mf, sample_config, s3_resource, manifest=manifest)

    with patch("databudgie.backup.log") as mock_log:
        test_backup_all(pg, mf, sample_config, s3_resource, manifest=manifest)
        assert mock_log.info.call_count == 2
        mock_log.info.assert_has_calls([call("Skipping public.store..."), call("Skipping public.customer...")])


def test_manifest_restore(pg, mf, s3_resource):
    manifest = RestoreManifest(pg, DatabudgieManifest.__tablename__)
    test_restore_one(pg, mf, s3_resource, manifest=manifest)

    row = pg.query(DatabudgieManifest).first()

    assert row.transaction == 1
    assert row.table == "public.product"
    assert row.file_path == "s3://sample-bucket/products.csv"


def test_manifest_restore_resume_transaction(pg, s3_resource, sample_config):
    manifest = RestoreManifest(pg, DatabudgieManifest.__tablename__)
    manifest.set_transaction_id(999)  # arbitary transaction id for better coverage

    test_restore_all(pg, sample_config, s3_resource, manifest=manifest)

    with patch("databudgie.restore.log") as mock_log:
        test_restore_all(pg, sample_config, s3_resource, manifest=manifest)
        assert mock_log.info.call_count == 2
        mock_log.info.assert_has_calls([call("Skipping public.store..."), call("Skipping public.product...")])


def test_manifest_subsequent_transaction(pg, mf):
    mf.manifest.backup(transaction=1, table="public.user", file_path="s3://sample-bucket/users.csv")

    manifest = BackupManifest(pg, DatabudgieManifest.__tablename__)
    manifest.record("public.user", "s3://sample-bucket/users.csv")

    rows = pg.query(DatabudgieManifest).all()
    assert len(rows) == 2
    assert rows[1].transaction == 2
