from typing import Iterable, Optional, Tuple

import click
from setuplog import log
from sqlalchemy.orm import Session

from databudgie.cli.base import resolver
from databudgie.cli.config import CliConfig, DEFAULT_CONFIG_FILE, load_configs
from databudgie.config.models import BackupConfig, ConfigError, ConfigStack, RestoreConfig, RootConfig
from databudgie.manifest.manager import Manifest


@resolver.group()
@click.option("--strict/--no-strict", is_flag=True, default=False)
@click.option("-a", "--adapter", default=None, help="postgres, python, etc.")
@click.option("-c", "--config", default=[DEFAULT_CONFIG_FILE], help="config file", multiple=True)
@click.option("-v", "--verbose", count=True, default=0)
@click.option(
    "--ddl", default=None, is_flag=True, help="Whether to backup the DDL. Overrides the config option, if set"
)
@click.option("-u", "--url", default=None, help="The url used to connect to the database.")
@click.option("-l", "--location", default=None, help="The location to read/write backups from/to.")
@click.option(
    "-t",
    "--table",
    default=None,
    multiple=True,
    help="The set of tables to backup or restore. Note that this overrides any table section in config.",
)
@click.version_option()
def cli(
    strict: bool,
    adapter: str,
    config: Iterable[str],
    verbose: int,
    ddl: Optional[bool] = None,
    url: Optional[str] = None,
    table: Optional[Tuple[str, ...]] = None,
    location: Optional[str] = None,
):
    cli_config = CliConfig(
        ddl=ddl,
        tables=list(table) if table else None,
        url=url,
        location=location,
    )

    configs = load_configs(config)
    config_stack = ConfigStack(cli_config.to_dict(), *configs)

    try:
        root_config = RootConfig.from_stack(config_stack)
    except ConfigError as e:
        raise click.UsageError(*e.args)

    resolver.register_values(
        adapter=adapter,
        root_config=root_config,
        strict=strict,
        verbosity=verbose,
    )


@resolver.command(cli, "backup")
@click.option("--backup-id", default=None, help="Backup manifest id.")
def backup_cli(
    backup_config: BackupConfig,
    backup_db: Session,
    strict: bool,
    adapter: str,
    verbosity: int,
    backup_manifest: Optional[Manifest] = None,
    backup_id: Optional[int] = None,
):
    """Perform backup."""
    from databudgie.cli.setup import setup
    from databudgie.etl.backup import backup_all

    setup(backup_config.sentry, backup_config.logging, verbosity=verbosity)

    if backup_manifest and backup_id:
        backup_manifest.set_transaction_id(backup_id)

    log.info("Performing backup!")
    backup_all(backup_db, backup_config, manifest=backup_manifest, strict=strict, adapter=adapter)


@resolver.command(cli, "restore")
@click.option("--restore-id", default=None, help="Restore manifest id.")
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
    clean: Optional[bool] = None,
    yes: bool = False,
):
    """Perform restore."""
    from databudgie.cli.setup import setup
    from databudgie.etl.restore import restore_all

    setup(restore_config.sentry, restore_config.logging, verbosity=verbosity)

    if restore_manifest and restore_id:
        restore_manifest.set_transaction_id(restore_id)

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
