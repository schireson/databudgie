from unittest.mock import patch

from databudgie.adapter import Adapter
from databudgie.backup import backup_all
from databudgie.config import RootConfig
from tests.adapter import test_postgres
from tests.test_backup import _validate_backup_contents
from tests.test_restore import test_restore_one
from tests.utils import get_file_buffer, s3_config


def test_backup(pg, mf, s3_resource):
    with patch("databudgie.backup.Adapter.get_adapter", return_value=Adapter(pg)):
        customer = mf.customer.new(external_id="cid_123")

        config = RootConfig.from_dict(
            {
                "tables": {"public.customer": {"location": "s3://sample-bucket/public.customer"}},
                "sequences": False,
                **s3_config,
            }
        )
        backup_all(pg, config.backup)

        _validate_backup_contents(
            get_file_buffer("s3://sample-bucket/public.customer/2021-04-26T09:00:00.csv", s3_resource),
            [customer],
        )


def test_restore(pg, mf, s3_resource):
    with patch("databudgie.restore.Adapter.get_adapter", return_value=Adapter(pg)):
        test_restore_one(pg, mf, s3_resource)


def test_type_conversion(pg, mf, s3_resource):
    with patch("databudgie.backup.Adapter.get_adapter", return_value=Adapter(pg)):
        test_postgres.test_type_conversion(pg, mf, s3_resource)
