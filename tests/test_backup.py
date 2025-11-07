import csv
import gzip
import io
import tempfile
from typing import Any, Dict, List
from unittest.mock import patch

import faker
import pytest

from databudgie.backup import backup_all
from databudgie.config import RootConfig
from tests.mockmodels.models import Customer
from tests.utils import get_file_buffer, s3_config

fake = faker.Faker()


def test_backup_all(pg, s3_resource):
    """Validate the backup_all performs backup for all tables in the backup config."""
    config = RootConfig.from_dict(
        {
            "location": "s3://sample-bucket/databudgie/test/{table}",
            "tables": ["public.customer", "public.store"],
            "sequences": False,
            "strict": True,
            **s3_config,
        }
    )

    backup_all(pg, config.backup)

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
            "strict": True,
            **s3_config,
        }
    )

    backup_all(pg, backup_config=config.backup)

    all_object_keys = [obj.key for obj in s3_resource.Bucket("sample-bucket").objects.all()]
    assert all_object_keys == [
        "databudgie/test/public.customer/2021-04-26T09:00:00.csv",
        "databudgie/test/public.store/2021-04-26T09:00:00.csv",
    ]


def test_backup_all_tables_list(pg, s3_resource):
    config = RootConfig.from_dict(
        {
            **s3_config,
            "strict": True,
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

    backup_all(pg, backup_config=config.backup)

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
@pytest.mark.parametrize("adapter", ("postgres", "python"))
def test_compression(pg, s3_resource, config, adapter):
    config = RootConfig.from_dict({"backup": config, "sequences": False, "strict": True, **s3_config})
    backup_all(pg, backup_config=config.backup)

    all_objects = list(s3_resource.Bucket("sample-bucket").objects.all())
    assert len(all_objects) == 1

    obj = all_objects[0]
    assert obj.key == "databudgie/test/public.product/2021-04-26T09:00:00.csv.gz"

    content = obj.get()["Body"].read()
    assert gzip.decompress(content) == b"id,store_id,external_id,external_name,external_status,active\n"


def test_backup_local_file(pg, mf):
    """Validate the upload for a single table contains the correct contents."""
    customer = mf.customer.new(external_id="cid_123")

    with tempfile.TemporaryDirectory() as dir_name:
        config = RootConfig.from_dict({"tables": {"public.customer": {"location": f"{dir_name}/public.customer"}}})
        backup_all(pg, config.backup)

        _validate_backup_contents(get_file_buffer(f"{dir_name}/public.customer/2021-04-26T09:00:00.csv"), [customer])


def test_backup_s3(pg, mf, s3_resource):
    """Validate the upload for a single table contains the correct contents."""
    customer = mf.customer.new(external_id="cid_123")

    config = RootConfig.from_dict(
        {"tables": {"public.customer": {"location": "s3://sample-bucket/databudgie/test/public.customer"}}, **s3_config}
    )
    backup_all(pg, config.backup)

    _validate_backup_contents(
        get_file_buffer("s3://sample/databudgie/test/public.customer/2021-04-26T09:00:00.csv", s3_resource), [customer]
    )


def test_backup_failure(pg):
    """Validate alternative behavior of the `strict` flag."""
    config = {
        "location": "s3://sample-bucket/databudgie/test/{table}",
        "tables": ["public.customer", "public.store"],
        "sequences": False,
        **s3_config,
    }
    with patch("databudgie.backup.backup", side_effect=RuntimeError("Dummy error")):
        # With strict on, the backup should raise an exception.
        with pytest.raises(RuntimeError):
            strict_config = RootConfig.from_dict({**config, "strict": True})
            backup_all(pg, backup_config=strict_config.backup)

        # With strict off, the backup should produce log messages.
        with patch("databudgie.output.Console.exception") as console:
            loose_config = RootConfig.from_dict({**config, "strict": False})
            backup_all(pg, backup_config=loose_config.backup)
            assert console.call_count == 2


def test_backup_unnamed_table(pg, mf, s3_resource):
    """Validate unnamed table can be backed up."""
    customer = mf.customer.new(external_id="cid_123")

    config = RootConfig.from_dict(
        {
            "backup": {
                "location": "s3://sample-bucket/databudgie/test/{table}",
                "tables": [{"query": "select * from public.customer"}],
                **s3_config,
            },
        }
    )
    backup_all(pg, config.backup)

    _validate_backup_contents(
        get_file_buffer("s3://sample/databudgie/test/None/2021-04-26T09:00:00.csv", s3_resource), [customer]
    )


def test_backup_with_idle_in_transaction_timeout(pg, s3_resource):
    """Validate backup works with idle_in_transaction_timeout config option."""
    config = RootConfig.from_dict(
        {
            "location": "s3://sample-bucket/databudgie/test/{table}",
            "tables": ["public.customer", "public.store"],
            "sequences": False,
            "strict": True,
            "idle_in_transaction_timeout": 5000,
            **s3_config,
        }
    )

    backup_all(pg, config.backup)

    all_object_keys = [obj.key for obj in s3_resource.Bucket("sample-bucket").objects.all()]
    assert all_object_keys == [
        "databudgie/test/public.customer/2021-04-26T09:00:00.csv",
        "databudgie/test/public.store/2021-04-26T09:00:00.csv",
    ]


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


def _comparable_bool(value):
    """Convert a boolean value to a comparable value."""
    if value is True:
        return (True, "True", "t", "true")

    if value is False:
        return (False, "False", "f", "false")

    raise ValueError(f"Invalid boolean value: {value}")
