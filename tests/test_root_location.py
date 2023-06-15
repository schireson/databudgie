import tempfile

import pytest

from databudgie.backup import backup_all
from databudgie.config import RootConfig
from tests.utils import get_file_buffer, s3_config


def test_backup_local_file(pg, mf):
    """Verify local files compose correctly with the 'root_location' option."""
    mf.store.new(id=1, name="example")

    with tempfile.TemporaryDirectory() as dir_name:
        config = RootConfig.from_dict(
            {"root_location": dir_name, "tables": {"public.store": {"location": "public.store/"}}}
        )
        backup_all(pg, config.backup)

        content = get_file_buffer(f"{dir_name}/public.store/2021-04-26T09:00:00.csv").read()
    assert content == b"id,name\n1,example\n"


def test_backup_s3_file_relative_local_path(pg, mf, s3_resource):
    """Verify s3 files compose correctly with the 'root_location' option."""
    mf.store.new(id=1, name="example")

    config = RootConfig.from_dict(
        {
            "root_location": "s3://sample-bucket/root-path/",
            "tables": {"public.store": {"location": "/public.store/"}},
            "sequences": False,
            **s3_config,
        }
    )
    backup_all(pg, config.backup)

    content = get_file_buffer("s3://sample-bucket/root-path/public.store/2021-04-26T09:00:00.csv", s3_resource).read()
    assert content == b"id,name\n1,example\n"


def test_backup_relative_root_absolute_location(pg, mf, s3_resource):
    mf.store.new(id=1, name="example")

    config = RootConfig.from_dict(
        {
            "root_location": "root-path/",
            "tables": {"public.store": {"location": "s3://sample-bucket/public.store/"}},
            "sequences": False,
            **s3_config,
        }
    )
    backup_all(pg, config.backup)

    content = get_file_buffer("s3://sample-bucket/root-path/public.store/2021-04-26T09:00:00.csv", s3_resource).read()
    assert content == b"id,name\n1,example\n"


def test_backup_s3_file_absolute_local_path(pg, mf, s3_resource):
    """Verify s3 files compose correctly with the 'root_location' option.

    An absolute path to the same bucket should transparently compose the paths together.
    """
    mf.store.new(id=1, name="example")

    config = RootConfig.from_dict(
        {
            "root_location": "s3://sample-bucket/root-path/",
            "tables": {"public.store": {"location": "s3://sample-bucket/public.store/"}},
            **s3_config,
        }
    )
    backup_all(pg, config.backup)

    content = get_file_buffer("s3://sample-bucket/root-path/public.store/2021-04-26T09:00:00.csv", s3_resource).read()
    assert content == b"id,name\n1,example\n"


def test_backup_s3_file_invalid_bucket(mf):
    """Verify s3 files compose correctly with the 'root_location' option.

    Different buckets on the composed paths should fail.
    """
    mf.store.new(id=1, name="example")

    with pytest.raises(ValueError):
        RootConfig.from_dict(
            {
                "root_location": "s3://sample-bucket/root-path/",
                "tables": {"public.store": {"location": "s3://other-bucket/public.store/"}},
                **s3_config,
            }
        )
