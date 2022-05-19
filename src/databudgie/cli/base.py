from typing import Optional

import click
import sqlalchemy.orm
import strapp.click
import strapp.logging
from configly import Config
from setuplog import log
from sqlalchemy.orm import Session

from databudgie.config.models import BackupConfig, RestoreConfig, RootConfig
from databudgie.manifest.manager import Manifest


def _create_postgres_session(url):
    engine = sqlalchemy.create_engine(url)
    session = sqlalchemy.orm.scoping.scoped_session(sqlalchemy.orm.session.sessionmaker(bind=engine))()
    return session


def backup_config(root_config: RootConfig):
    if not root_config.backup:
        log.error("No backup config found. Run 'databudgie config' to see your current configuration.")
        exit(1)
    return root_config.backup


def restore_config(root_config: RootConfig):
    if not root_config.restore:
        log.error("No restore config found. Run 'databudgie config' to see your current configuration.")
        exit(1)
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


@resolver.group()
@click.option("--strict/--no-strict", is_flag=True, default=False)  # TODO: consider pre-check functionality
@click.option("-a", "--adapter", default=None, help="postgres, python, etc.")
@click.option("-c", "--config", default="config.databudgie.yml", help="config file")
@click.option("-v", "--verbose", count=True, default=0)
def cli(strict: bool, adapter: str, config: str, verbose: int):
    root_config = RootConfig.from_dict(Config.from_yaml(config).to_dict())
    resolver.register_values(
        adapter=adapter,
        root_config=root_config,
        strict=strict,
        verbosity=verbose,
    )


@resolver.command(cli, "backup")
@click.option("--backup-id", default=None, help="Restore manifest id.")
@click.option(
    "--ddl", default=None, is_flag=True, help="Whether to backup the DDL. Overrides the config option, if set"
)
def backup_cli(
    backup_config: BackupConfig,
    backup_db: Session,
    strict: bool,
    adapter: str,
    verbosity: int,
    backup_manifest: Optional[Manifest] = None,
    backup_id: Optional[int] = None,
    ddl: Optional[bool] = None,
):
    """Perform backup."""
    from databudgie.cli.setup import setup
    from databudgie.etl.backup import backup_all

    setup(backup_config.sentry, backup_config.logging, verbosity=verbosity)

    if backup_manifest and backup_id:
        backup_manifest.set_transaction_id(backup_id)

    backup_config.ddl.enabled = ddl or backup_config.ddl.enabled

    log.info("Performing backup!")
    backup_all(backup_db, backup_config, manifest=backup_manifest, strict=strict, adapter=adapter)


@resolver.command(cli, "restore")
@click.option("--restore-id", default=None, help="Restore manifest id.")
@click.option(
    "--ddl", default=None, is_flag=True, help="Whether to backup the DDL. Overrides the config option, if set"
)
@click.option(
    "--clean/--no-clean",
    is_flag=True,
    default=None,
    help="Drops and recreates the target database before performing the restore",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Dont ask for confirmation",
)
def restore_cli(
    restore_config: RestoreConfig,
    restore_db: Session,
    strict: bool,
    verbosity: int,
    restore_manifest: Optional[Manifest] = None,
    restore_id: Optional[int] = None,
    adapter: Optional[str] = None,
    ddl: Optional[bool] = None,
    clean: Optional[bool] = None,
    yes: bool = False,
):
    """Perform restore."""
    from databudgie.cli.setup import setup
    from databudgie.etl.restore import restore_all

    setup(restore_config.sentry, restore_config.logging, verbosity=verbosity)

    if restore_manifest and restore_id:
        restore_manifest.set_transaction_id(restore_id)

    restore_config.ddl.enabled = ddl or restore_config.ddl.enabled
    restore_config.ddl.clean = clean or restore_config.ddl.clean

    if not yes and restore_config.ddl.clean:
        message = "About to delete the database! input 'y' if that's what you want: "
        if input(message) != "y":  # nosec
            return False

    log.info("Performing restore!")

    restore_all(restore_db, restore_config=restore_config, manifest=restore_manifest, strict=strict, adapter=adapter)


@resolver.command(cli, "config")
def config_cli(root_config: RootConfig):
    """Print dereferenced and populated config."""

    from databudgie.config import pretty_print

    pretty_print(root_config)
