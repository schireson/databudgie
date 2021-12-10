import csv
import io
from typing import Any, Dict, List
from unittest.mock import patch

import pytest
from configly import Config

from databudgie.etl.backup import backup, backup_all
from databudgie.etl.base import TableOp
from tests.mockmodels.models import Customer


def test_backup_all(pg, mf, sample_config, s3_resource, **extras):
    """Validate the backup_all performs backup for all tables in the backup config."""

    backup_all(pg, s3_resource, config=sample_config, strict=True, **extras)

    all_object_keys = [obj.key for obj in s3_resource.Bucket("sample-bucket").objects.all()]
    assert all_object_keys == [
        "databudgie/test/public.customer/2021-04-26T09:00:00.csv",
        "databudgie/test/public.store/2021-04-26T09:00:00.csv",
    ]


def test_backup_all_glob(pg, s3_resource):
    config = Config(
        {
            "backup": {
                "tables": {
                    "public.*": {
                        "location": "s3://sample-bucket/databudgie/test/{table}",
                        "query": "select * from {table}",
                        "exclude": ["public.databudgie_*", "public.product", "public.sales"],
                    },
                }
            },
        }
    )
    backup_all(pg, s3_resource, config, strict=True)

    all_object_keys = [obj.key for obj in s3_resource.Bucket("sample-bucket").objects.all()]
    assert all_object_keys == [
        "databudgie/test/public.customer/2021-04-26T09:00:00.csv",
        "databudgie/test/public.store/2021-04-26T09:00:00.csv",
    ]


def test_backup_one(pg, mf, s3_resource, **extras):
    """Validate the upload for a single table contains the correct contents."""
    customer = mf.customer.new(external_id="cid_123")

    backup(
        pg,
        s3_resource=s3_resource,
        config=None,
        table_op=TableOp(
            "public.customer",
            dict(
                query="select * from public.customer",
                location="s3://sample-bucket/databudgie/test/public.customer",
            ),
        ),
        **extras,
    )

    _validate_backup_contents(s3_resource, "databudgie/test/public.customer/2021-04-26T09:00:00.csv", [customer])


def test_backup_failure(pg, sample_config):
    """Validate alternative behavior of the `strict` flag."""

    with patch("databudgie.etl.backup.backup", side_effect=RuntimeError("Dummy error")):
        # With strict on, the backup should raise an exception.
        with pytest.raises(RuntimeError):
            backup_all(pg, None, config=sample_config, strict=True)

        # With strict off, the backup should produce log messages.
        with patch("databudgie.utils.log") as mock_log:
            backup_all(pg, None, config=sample_config, strict=False)
            assert mock_log.info.call_count == 2


def _validate_backup_contents(s3_resource, s3_key, expected_contents: List[Customer]):
    """Validate the contents of a backup file. Columns from the file will be raw."""
    buffer = io.BytesIO()
    uploaded_object = s3_resource.Object("sample-bucket", s3_key)
    uploaded_object.download_fileobj(buffer)
    buffer.seek(0)

    wrapper = io.TextIOWrapper(buffer, encoding="utf-8")
    reader = csv.DictReader(wrapper)
    contents: List[Dict[str, Any]] = list(reader)

    assert len(contents) == len(expected_contents)
    for actual, expected in zip(contents, expected_contents):
        assert actual["external_id"] == expected.external_id
        assert actual["store_id"] == str(expected.store_id)
        assert actual["product_id"] == str(expected.product_id)
        assert actual["external_name"] == expected.external_name
        assert actual["type"] == expected.type
        assert actual["active"] in _comparable_bool(expected.active)
        assert actual["external_status"] == expected.external_status


def _comparable_bool(value: bool):
    """Convert a boolean value to a comparable value."""
    if value is True:
        return (True, "True", "t", "true")
    elif value is False:
        return (False, "False", "f", "false")
    else:
        raise ValueError(f"Invalid boolean value: {value}")
