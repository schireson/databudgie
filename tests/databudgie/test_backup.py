import csv
import io
from unittest.mock import patch

import pytest

from databudgie.backup import backup, backup_all


def test_backup_all(pg, mf, sample_config, s3_resource):
    """Validate the backup_all performs backup for all tables in the backup config."""

    backup_all(pg, s3_resource, tables=sample_config.backup.tables, strict=True)

    all_object_keys = [obj.key for obj in s3_resource.Bucket("sample-bucket").objects.all()]
    assert all_object_keys == [
        "databudgie/test/public.ad_generic.csv",
        "databudgie/test/public.advertiser.csv",
    ]


def test_backup_one(pg, mf, s3_resource):
    """Validate the upload for a single table contains the correct contents."""
    mf.facebook_ad.new(external_id="ad_123")

    backup(
        pg,
        query="select * from public.ad_generic",
        s3_resource=s3_resource,
        location="s3://sample-bucket/databudgie/test/public.ad_generic.csv",
        table_name="public.ad_generic",
    )

    buffer = io.BytesIO()
    uploaded_object = s3_resource.Object("sample-bucket", "databudgie/test/public.ad_generic.csv")
    uploaded_object.download_fileobj(buffer)
    buffer.seek(0)

    wrapper = io.TextIOWrapper(buffer, encoding="utf-8")
    reader = csv.DictReader(wrapper)

    contents = list(reader)
    assert len(contents) == 1
    assert contents[0]["external_id"] == "ad_123"


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
