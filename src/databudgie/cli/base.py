from typing import Optional

import click
import sqlalchemy.orm
import strapp.click
import strapp.logging
from configly import Config
from mypy_boto3_s3 import S3ServiceResource
from setuplog import log
from sqlalchemy.orm import Session

from databudgie.manifest.manager import Manifest


def _load_config(config_file: str) -> Config:
    from databudgie.config import populate_refs

    base_config = Config.from_yaml(config_file)
    return populate_refs(base_config)


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


def s3_resource(config) -> S3ServiceResource:
    import boto3

    session = boto3.session.Session(
        aws_access_key_id=config.s3.aws_access_key_id,
        aws_secret_access_key=config.s3.aws_secret_access_key,
        profile_name=config.s3.profile,
        region_name=config.s3.region,
    )
    s3: S3ServiceResource = session.resource("s3")
    return s3


resolver = strapp.click.Resolver(
    backup_db=backup_db,
    restore_db=restore_db,
    backup_manifest=backup_manifest,
    restore_manifest=restore_manifest,
    s3_resource=s3_resource,
)


@resolver.group()
@click.option("--strict/--no-strict", is_flag=True, default=False)  # TODO: consider pre-check functionality
@click.option("-a", "--adapter", default=None, help="postgres, python, etc.")
@click.option("-c", "--config", default="config.databudgie.yml", help="config file")
@click.option("-v", "--verbose", count=True, default=0)
def cli(strict: bool, adapter: str, config: str, verbose: int):
    from databudgie.cli.setup import setup

    conf: Config = _load_config(config)

    setup(conf, verbosity=verbose)
    resolver.register_values(adapter=adapter, config=conf, strict=strict, verbosity=verbose)


@resolver.command(cli, "backup")
@click.option("--backup-id", default=None, help="Restore manifest id.")
def backup_cli(
    config: Config,
    backup_db: Session,
    s3_resource: S3ServiceResource,
    strict: bool,
    adapter: str,
    backup_manifest: Optional[Manifest] = None,
    backup_id: Optional[int] = None,
):
    """Perform backup."""
    from databudgie.etl.backup import backup_all

    if backup_manifest and backup_id:
        backup_manifest.set_transaction_id(backup_id)

    log.info("Performing backup! (environment: %s)", config.environment)
    backup_all(backup_db, s3_resource, config, manifest=backup_manifest, strict=strict, adapter=adapter)


@resolver.command(cli, "restore")
@click.option("--restore-id", default=None, help="Restore manifest id.")
def restore_cli(
    config: Config,
    restore_db: Session,
    s3_resource: S3ServiceResource,
    strict: bool,
    restore_manifest: Optional[Manifest] = None,
    restore_id: Optional[int] = None,
    adapter: str = None,
):
    """Perform restore."""
    from databudgie.etl.restore import restore_all

    if restore_manifest and restore_id:
        restore_manifest.set_transaction_id(restore_id)

    log.info("Performing restore! (environment: %s)", config.environment)
    restore_all(restore_db, s3_resource, config, manifest=restore_manifest, strict=strict, adapter=adapter)


@resolver.command(cli, "config")
@click.option("-i", "--indent", default=2, help="Indentation level.")
def config_cli(config: Config, indent: int):
    """Print dereferenced and populated config."""

    from databudgie.config import pretty_print

    pretty_print(config, increment=indent)
