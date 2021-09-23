from unittest.mock import patch

from databudgie.adapter.fallback import PythonAdapter
from tests.databudgie.test_backup import test_backup_one
from tests.databudgie.test_restore import mock_bucket, test_restore_one  # noqa


def test_backup(pg, mf, s3_resource):
    with patch("databudgie.backup.Adapter.get_adapter", return_value=PythonAdapter()):
        test_backup_one(pg, mf, s3_resource)


def test_restore(pg, mf, s3_resource, mock_bucket):  # noqa
    with patch("databudgie.restore.Adapter.get_adapter", return_value=PythonAdapter()):
        test_restore_one(pg, mf, s3_resource, mock_bucket)
