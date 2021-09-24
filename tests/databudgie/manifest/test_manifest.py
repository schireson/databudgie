from databudgie.manifest.manager import BackupManifest, RestoreManifest
from tests.databudgie.test_backup import test_backup_one
from tests.databudgie.test_restore import test_restore_one
from tests.mockmodels.models import DatabudgieBackup, DatabudgieRestore


def test_manifest_backup(pg, mf, s3_resource):
    manifest = BackupManifest(pg, DatabudgieBackup.__tablename__)
    test_backup_one(pg, mf, s3_resource, manifest=manifest)

    row = pg.query(DatabudgieBackup).first()

    assert row.transaction == 1
    assert row.table == "public.ad_generic"
    assert row.file_path == "s3://sample-bucket/databudgie/test/public.ad_generic.csv"


def test_manifest_restore(pg, mf, s3_resource):
    manifest = RestoreManifest(pg, DatabudgieRestore.__tablename__)
    test_restore_one(pg, mf, s3_resource, manifest=manifest)

    row = pg.query(DatabudgieRestore).first()

    assert row.transaction == 1
    assert row.table == "public.product"
    assert row.file_path == "s3://sample-bucket/products.csv"


# TODO: Add test for skipping tables
# TODO: Add test for resuming transactions
