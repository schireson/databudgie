import csv
import gzip
import io
import tempfile
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from databudgie.adapter.base import Adapter
from databudgie.config.models import BackupTableConfig, RootConfig
from databudgie.etl.backup import backup, backup_all
from databudgie.etl.base import TableOp
from databudgie.s3 import is_s3_path, S3Location
from tests.mockmodels.models import Customer
from tests.utils import s3_config


def test_backup_all(pg, s3_resource, **extras):
    """Validate the backup_all performs backup for all tables in the backup config."""

    config = RootConfig.from_dict(
        {
            "location": "s3://sample-bucket/databudgie/test/{table}",
            "tables": ["public.customer", "public.store"],
            "sequences": False,
            **s3_config,
        }
    )

    backup_all(pg, config.backup, strict=True, **extras)

    all_object_keys = [obj.key for obj in s3_resource.Bucket("sample-bucket").objects.all()]
    assert all_object_keys == [
        "databudgie/test/public.customer/2021-04-26T09:00:00.csv",
        "databudgie/test/public.store/2021-04-26T09:00:00.csv",
    ]


def test_backup_all_glob(pg, s3_resource):

    config = RootConfig.from_dict(
        {
            "tables": {
                "public.*": {
                    "location": "s3://sample-bucket/databudgie/test/{table}",
                    "exclude": ["public.databudgie_*", "public.product", "public.sales"],
                }
            },
            "sequences": False,
            **s3_config,
        }
    )

    backup_all(pg, backup_config=config.backup, strict=True)

    all_object_keys = [obj.key for obj in s3_resource.Bucket("sample-bucket").objects.all()]
    assert all_object_keys == [
        "databudgie/test/public.customer/2021-04-26T09:00:00.csv",
        "databudgie/test/public.store/2021-04-26T09:00:00.csv",
    ]


def test_backup_all_tables_list(pg, s3_resource):
    config = RootConfig.from_dict(
        {
            **s3_config,
            "sequences": False,
            "backup": {
                "tables": [
                    {
                        "name": "public.product",
                        "location": "s3://sample-bucket/databudgie/test/{table}_not_4",
                        "query": "select * from {table} where id != 4",
                    },
                    {
                        "name": "public.product",
                        "location": "s3://sample-bucket/databudgie/test/{table}",
                        "query": "select * from {table} where id = 4",
                    },
                ],
            },
        }
    )

    backup_all(pg, backup_config=config.backup, strict=True)

    all_object_keys = [obj.key for obj in s3_resource.Bucket("sample-bucket").objects.all()]
    assert all_object_keys == [
        "databudgie/test/public.product/2021-04-26T09:00:00.csv",
        "databudgie/test/public.product_not_4/2021-04-26T09:00:00.csv",
    ]


@pytest.mark.parametrize(
    "config",
    (
        {
            "compression": "gzip",
            "tables": {
                "public.product": {
                    "location": "s3://sample-bucket/databudgie/test/{table}",
                },
            },
        },
        {
            "tables": {
                "public.product": {
                    "location": "s3://sample-bucket/databudgie/test/{table}",
                    "compression": "gzip",
                },
            },
        },
    ),
)
def test_compression(pg, s3_resource, config):
    config = RootConfig.from_dict({"backup": config, "sequences": False, **s3_config})
    backup_all(pg, backup_config=config.backup, strict=True)

    all_objects = [obj for obj in s3_resource.Bucket("sample-bucket").objects.all()]
    assert len(all_objects) == 1

    obj = all_objects[0]
    assert obj.key == "databudgie/test/public.product/2021-04-26T09:00:00.csv.gz"

    content = obj.get()["Body"].read()
    assert gzip.decompress(content) == b"id,store_id,external_id,external_name,external_status,active\n"


def test_backup_local_file(pg, mf, **extras):
    """Validate the upload for a single table contains the correct contents."""
    customer = mf.customer.new(external_id="cid_123")

    with tempfile.TemporaryDirectory() as dir_name:
        backup(
            pg,
            adapter=Adapter.get_adapter(pg),
            table_op=TableOp(
                "public.customer",
                BackupTableConfig(
                    name="public.customer",
                    query="select * from public.customer",
                    location=f"{dir_name}/public.customer",
                ),
            ),
            **extras,
        )

        _validate_backup_contents(_get_file_buffer(f"{dir_name}/public.customer/2021-04-26T09:00:00.csv"), [customer])


def test_backup_one(pg, mf, s3_resource, **extras):
    """Validate the upload for a single table contains the correct contents."""
    customer = mf.customer.new(external_id="cid_123")

    backup(
        pg,
        s3_resource=s3_resource,
        adapter=Adapter.get_adapter(pg),
        table_op=TableOp(
            "public.customer",
            BackupTableConfig(
                name="public.customer",
                query="select * from public.customer",
                location="s3://sample-bucket/databudgie/test/public.customer",
            ),
        ),
        **extras,
    )

    _validate_backup_contents(
        _get_file_buffer("s3://sample/databudgie/test/public.customer/2021-04-26T09:00:00.csv", s3_resource), [customer]
    )


def test_backup_failure(pg):
    """Validate alternative behavior of the `strict` flag."""

    config = RootConfig.from_dict(
        {
            "location": "s3://sample-bucket/databudgie/test/{table}",
            "tables": ["public.customer", "public.store"],
            "sequences": False,
            **s3_config,
        }
    )

    with patch("databudgie.etl.backup.backup", side_effect=RuntimeError("Dummy error")):
        # With strict on, the backup should raise an exception.
        with pytest.raises(RuntimeError):
            backup_all(pg, backup_config=config.backup, strict=True)

        # With strict off, the backup should produce log messages.
        with patch("databudgie.output.Console.exception") as console:
            backup_all(pg, backup_config=config.backup, strict=False)
            assert console.call_count == 2


def _get_file_buffer(filename, s3_resource=None):
    buffer = io.BytesIO()

    if is_s3_path(filename) and s3_resource:
        location = S3Location(filename)
        uploaded_object = s3_resource.Object("sample-bucket", location.key)
        uploaded_object.download_fileobj(buffer)
    else:
        with open(filename, "rb") as f:
            buffer.write(f.read())
    buffer.seek(0)

    return buffer


def _validate_backup_contents(buffer, expected_contents: List[Customer]):
    """Validate the contents of a backup file. Columns from the file will be raw."""

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
