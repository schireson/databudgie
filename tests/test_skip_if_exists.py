import pathlib
import tempfile
from dataclasses import replace

import faker

from databudgie.backup import backup_all
from databudgie.config import RootConfig
from tests.utils import mock_csv, mock_s3_csv, s3_config

fake = faker.Faker()


def test_skip_if_exists_in_s3(pg, s3_resource):
    """Validate the `skip_if_exists` backup option skips tables with existing backups."""
    path = "public.store"
    file = f"{path}/2021-04-26T09:00:00.csv"

    mock_store = {"id": 1, "name": fake.name()}
    mock_s3_csv(s3_resource, file, [mock_store])

    config = RootConfig.from_dict(
        {
            "location": "s3://sample-bucket/{table}",
            "tables": ["public.store"],
            "sequences": False,
            "strict": True,
            "skip_if_exists": True,
            **s3_config,
        }
    )

    existing_etag = s3_resource.Object("sample-bucket", file).e_tag

    backup_all(pg, config.backup)

    skipped_etag = s3_resource.Object("sample-bucket", file).e_tag
    assert existing_etag == skipped_etag

    config.backup.tables[0] = replace(config.backup.tables[0], skip_if_exists=False)
    backup_all(pg, config.backup)

    not_skipped_etag = s3_resource.Object("sample-bucket", file).e_tag
    assert existing_etag != not_skipped_etag


def test_skip_if_exists_in_local_file(pg, s3_resource):
    """Validate the `skip_if_exists` backup option skips tables with existing backups."""
    mock_store = {"id": 1, "name": fake.name()}
    fake_file_data = mock_csv([mock_store]).read()

    with tempfile.TemporaryDirectory() as dir_name:
        path = pathlib.Path(dir_name, "2021-04-26T09:00:00.csv.csv")
        path.write_bytes(fake_file_data)

        config = RootConfig.from_dict(
            {
                "location": f"{dir_name}/{{table}}",
                "tables": ["public.store"],
                "sequences": False,
                "strict": True,
                "skip_if_exists": True,
            }
        )

        backup_all(pg, config.backup)

        skipped_bytes = path.read_bytes()
        assert fake_file_data == skipped_bytes

        config.backup.tables[0] = replace(config.backup.tables[0], skip_if_exists=False)
        backup_all(pg, config.backup)

        not_skipped_bytes = path.read_bytes()
        assert fake_file_data == not_skipped_bytes
