import csv
import io
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from databudgie.backup import backup, backup_all
from tests.mockmodels.models import GenericAd


def test_backup_all(pg, mf, sample_config, s3_resource, **extras):
    """Validate the backup_all performs backup for all tables in the backup config."""

    backup_all(pg, s3_resource, tables=sample_config.backup.tables, strict=True, **extras)

    all_object_keys = [obj.key for obj in s3_resource.Bucket("sample-bucket").objects.all()]
    assert all_object_keys == [
        "databudgie/test/public.ad_generic.csv",
        "databudgie/test/public.store.csv",
    ]


def test_backup_one(pg, mf, s3_resource, **extras):
    """Validate the upload for a single table contains the correct contents."""
    ad = mf.generic_ad.new(external_id="ad_123")

    backup(
        pg,
        query="select * from public.ad_generic",
        s3_resource=s3_resource,
        location="s3://sample-bucket/databudgie/test/public.ad_generic.csv",
        table_name="public.ad_generic",
        **extras,
    )

    _validate_backup_contents(s3_resource, "databudgie/test/public.ad_generic.csv", [ad])


def test_backup_failure(sample_config):
    """Validate alternative behavior of the `strict` flag."""

    with patch("databudgie.backup.backup", side_effect=RuntimeError("Dummy error")):
        # With strict on, the backup should raise an exception.
        with pytest.raises(RuntimeError):
            backup_all(None, None, tables=sample_config.backup.tables, strict=True)

        # With strict off, the backup should produce log messages.
        with patch("databudgie.utils.log") as mock_log:
            backup_all(None, None, tables=sample_config.backup.tables, strict=False)
            assert mock_log.info.call_count == 2


def _validate_backup_contents(s3_resource, s3_key, expected_contents: List[GenericAd]):
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
        assert actual["primary_text"] == expected.primary_text
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
