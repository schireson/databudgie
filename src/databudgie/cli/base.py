from typing import Optional

import click
import sqlalchemy.orm
import strapp.click
import strapp.logging
from configly import Config
from setuplog import log
from sqlalchemy.orm import Session

from databudgie.config import compose_value
from databudgie.manifest.manager import Manifest


def _create_postgres_session(url):
    engine = sqlalchemy.create_engine(url)
    session = sqlalchemy.orm.scoping.scoped_session(sqlalchemy.orm.session.sessionmaker(bind=engine))()
    return session


def backup_db(config):
    return _create_postgres_session(config.backup.url)


def restore_db(config):
    return _create_postgres_session(config.restore.url)


def backup_manifest(config, backup_db):
    from databudgie.manifest.manager import BackupManifest

    table_name: Optional[str] = config.backup.get("manifest")
    if table_name:
        return BackupManifest(backup_db, table_name)
    return None


def restore_manifest(config, restore_db):
    from databudgie.manifest.manager import RestoreManifest

    table_name: Optional[str] = config.restore.get("manifest")
    if table_name:
        return RestoreManifest(restore_db, table_name)
    return None


resolver = strapp.click.Resolver(
    backup_db=backup_db,
    restore_db=restore_db,
    backup_manifest=backup_manifest,
    restore_manifest=restore_manifest,
)


@resolver.group()
@click.option("--strict/--no-strict", is_flag=True, default=False)  # TODO: consider pre-check functionality
@click.option("-a", "--adapter", default=None, help="postgres, python, etc.")
@click.option("-c", "--config", default="config.databudgie.yml", help="config file")
@click.option("-v", "--verbose", count=True, default=0)
def cli(strict: bool, adapter: str, config: str, verbose: int):
    from databudgie.cli.setup import setup

    conf = Config.from_yaml(config)

    setup(conf, verbosity=verbose)
    resolver.register_values(adapter=adapter, config=conf, strict=strict, verbosity=verbose)


@resolver.command(cli, "backup")
@click.option("--backup-id", default=None, help="Restore manifest id.")
@click.option(
    "--ddl", default=None, is_flag=True, help="Whether to backup the DDL. Overrides the config option, if set"
)
def backup_cli(
    config: Config,
    backup_db: Session,
    strict: bool,
    adapter: str,
    backup_manifest: Optional[Manifest] = None,
    backup_id: Optional[int] = None,
    ddl: Optional[bool] = None,
):
    """Perform backup."""
    from databudgie.etl.backup import backup_all

    if backup_manifest and backup_id:
        backup_manifest.set_transaction_id(backup_id)

    config = compose_value(config, "backup", "ddl", "enabled", value=ddl, default=False)

    log.info("Performing backup! (environment: %s)", config.environment)
    backup_all(backup_db, config, manifest=backup_manifest, strict=strict, adapter=adapter)


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
    config: Config,
    restore_db: Session,
    strict: bool,
    restore_manifest: Optional[Manifest] = None,
    restore_id: Optional[int] = None,
    adapter: str = None,
    ddl: bool = None,
    clean: bool = None,
    yes: bool = False,
):
    """Perform restore."""
    from databudgie.etl.restore import restore_all

    if restore_manifest and restore_id:
        restore_manifest.set_transaction_id(restore_id)

    config = compose_value(config, "restore", "ddl", "enabled", value=ddl, default=False)
    config = compose_value(config, "restore", "ddl", "clean", value=clean, default=False)

    if not yes and config.restore.ddl.clean:
        message = "About to delete the database! input 'y' if that's what you want: "
        if input(message) != "y":  # nosec
            return False

    log.info("Performing restore! (environment: %s)", config.get("environment"))

    restore_all(restore_db, config=config, manifest=restore_manifest, strict=strict, adapter=adapter)


@resolver.command(cli, "config")
@click.option("-i", "--indent", default=2, help="Indentation level.")
def config_cli(config: Config, indent: int):
    """Print dereferenced and populated config."""

    from databudgie.config import pretty_print

    pretty_print(config, increment=indent)
