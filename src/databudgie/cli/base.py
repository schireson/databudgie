from typing import Optional, Union

import click
import sqlalchemy
import sqlalchemy.engine.url
import sqlalchemy.orm
import strapp.click
import strapp.logging

from databudgie.config import BackupConfig, RestoreConfig, RootConfig
from databudgie.output import Console

version = getattr(sqlalchemy, "__version__", "")
if version.startswith("1.4") or version.startswith("2."):
    create_url = sqlalchemy.engine.url.URL.create
else:
    create_url = sqlalchemy.engine.url.URL


def _create_postgres_session(url: Union[str, dict]):
    if isinstance(url, dict):
        url = create_url(**url)

    engine = sqlalchemy.create_engine(url)
    session = sqlalchemy.orm.scoping.scoped_session(sqlalchemy.orm.session.sessionmaker(bind=engine))()
    return session


def backup_config(root_config: RootConfig):
    if root_config.backup is None:
        raise click.UsageError("No backup config found. Run 'databudgie config' to see your current configuration.")

    if root_config.backup.url is None:
        raise click.UsageError(
            "No config found for 'url' field. Run 'databudgie config' to see your current configuration."
        )
    return root_config.backup


def restore_config(root_config: RootConfig, console: Console):
    if not root_config.restore:
        raise click.UsageError("No restore config found. Run 'databudgie config' to see your current configuration.")

    if not root_config.restore.url:
        raise click.UsageError(
            "No config found for 'url' field. Run 'databudgie config' to see your current configuration."
        )
    return root_config.restore


def backup_db(backup_config: BackupConfig):
    return _create_postgres_session(backup_config.url)


def restore_db(restore_config: RestoreConfig):
    return _create_postgres_session(restore_config.url)


def backup_manifest(backup_config: BackupConfig, backup_db):
    from databudgie.manifest.manager import BackupManifest

    table_name: Optional[str] = backup_config.manifest
    if table_name:
        return BackupManifest(backup_db, table_name)
    return None


def restore_manifest(restore_config: RestoreConfig, restore_db):
    from databudgie.manifest.manager import RestoreManifest

    table_name: Optional[str] = restore_config.manifest
    if table_name:
        return RestoreManifest(restore_db, table_name)
    return None


resolver = strapp.click.Resolver(
    backup_config=backup_config,
    backup_db=backup_db,
    backup_manifest=backup_manifest,
    restore_config=restore_config,
    restore_db=restore_db,
    restore_manifest=restore_manifest,
)
