from unittest.mock import patch

from databudgie.adapter.fallback import PythonAdapter
from tests.databudgie.adapter import test_postgres
from tests.databudgie.test_backup import test_backup_one
from tests.databudgie.test_restore import test_restore_one


def test_backup(pg, mf, s3_resource):
    with patch("databudgie.etl.backup.Adapter.get_adapter", return_value=PythonAdapter()):
        test_backup_one(pg, mf, s3_resource)


def test_restore(pg, mf, s3_resource):
    with patch("databudgie.etl.restore.Adapter.get_adapter", return_value=PythonAdapter()):
        test_restore_one(pg, mf, s3_resource)


def test_type_conversion(pg, mf, s3_resource):
    with patch("databudgie.etl.backup.Adapter.get_adapter", return_value=PythonAdapter()):
        test_postgres.test_type_conversion(pg, mf, s3_resource)
