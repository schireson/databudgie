import contextlib
import os
import pathlib
import tempfile
from unittest.mock import patch

import pytest
import sqlalchemy.exc
from configly import Config
from pytest_mock_resources import create_postgres_fixture

from databudgie.config.models import RootConfig
from databudgie.etl.backup import backup_all
from databudgie.etl.restore import restore_all
from tests.databudgie.ddl.models import Store

folder = pathlib.PurePath(__file__).parent

empty_db = create_postgres_fixture(session=True)


def tmp_dir():
    return tempfile.TemporaryDirectory(dir="")


@contextlib.contextmanager
def optionally_manage(manager=None, *, callable=None):
    if manager:
        yield manager
    else:
        with callable() as cm:
            yield cm


def test_backup_ddl_disabled(pg):
    with tmp_dir() as temp_dir:
        with patch("os.environ", new={"TABLE_LOCATION": temp_dir, "DDL_ENABLED": "false"}):
            config = Config.from_yaml(folder / "config.backup.yml")
            config = RootConfig.from_dict(config.to_dict())

        backup_all(pg, config.backup, strict=True)

        assert not os.path.exists(os.path.join(temp_dir, "ddl"))


def test_backup_ddl(pg, dir=None):
    with optionally_manage(dir, callable=tmp_dir) as temp_dir:
        os.environ["TABLE_LOCATION"] = temp_dir
        os.environ["DDL_ENABLED"] = "true"
        with patch("os.environ", new={"TABLE_LOCATION": temp_dir}):
            config = Config.from_yaml(folder / "config.backup.yml")
            config = RootConfig.from_dict(config.to_dict())

        backup_all(pg, config.backup, strict=True)

        assert os.path.exists(os.path.join(temp_dir, "ddl"))


def test_restore_ddl(pg, empty_db, mf):
    mf.store.new()
    mf.store.new()

    with optionally_manage(callable=tmp_dir) as temp_dir:
        test_backup_ddl(pg, dir=temp_dir)

        with patch("os.environ", new={"TABLE_LOCATION": temp_dir}):
            config = Config.from_yaml(folder / "config.restore.yml")
            config = RootConfig.from_dict(config.to_dict())

        restore_all(empty_db, config.restore, strict=True)
        empty_db.commit()

        rows = empty_db.query(Store).all()
        assert len(rows) == 2


def test_restore_ddl_disabled(pg, empty_db, mf):
    mf.store.new()
    mf.store.new()

    with optionally_manage(callable=tmp_dir) as temp_dir:
        test_backup_ddl(pg, dir=temp_dir)

        with patch("os.environ", new={"TABLE_LOCATION": temp_dir, "DDL_ENABLED": "false"}):
            config = Config.from_yaml(folder / "config.restore.yml")
            config = RootConfig.from_dict(config.to_dict())

        restore_all(empty_db, config.restore, strict=True)

        with pytest.raises(sqlalchemy.exc.ProgrammingError) as e:
            empty_db.query(Store).all()
        assert "does not exist" in str(e.value)
