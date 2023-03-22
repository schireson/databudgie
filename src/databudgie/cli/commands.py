import os
from typing import Iterable, Optional, Tuple

import click
from sqlalchemy.orm import Session

from databudgie.cli.base import resolver
from databudgie.cli.config import (
    CliConfig,
    DEFAULT_CONFIG_FILE,
    load_configs,
    pretty_print,
)
from databudgie.config import (
    BackupConfig,
    ConfigError,
    ConfigStack,
    RestoreConfig,
    RootConfig,
)
from databudgie.manifest.manager import Manifest
from databudgie.output import Console
from databudgie.storage import StorageBackend


@resolver.group()
@click.option("--strict/--no-strict", is_flag=True, default=None)
@click.option("--color/--no-color", is_flag=True, default=True)
@click.option(
    "-a",
    "--adapter",
    default=None,
    type=click.Choice(["postgres", "postgresql", "python"], case_sensitive=False),
    help="Override the automatic dialect detection.",
)
@click.option("-c", "--config", default=[DEFAULT_CONFIG_FILE], help="config file", multiple=True)
@click.option(
    "-C",
    "--conn",
    "--connection",
    default=None,
    help="The name of a named connection to use. Takes precedence over the 'url' field if present.",
)
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
@click.option(
    "-x",
    "--exclude",
    default=None,
    multiple=True,
    help="The set of tables to exclude. Note that this overrides any table section in config.",
)
@click.option(
    "--stats/--no-stats",
    default=None,
    is_flag=True,
    help="Print high level statistics about what the command did. Automatically implied by --dry-run!",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=None,
    is_flag=True,
    help="Do not actually perform the write operations of the backup/restore. It **does**, however, execute the queries.",
)
@click.version_option()
def cli(
    strict: bool,
    config: Iterable[str],
    verbose: int = 0,
    color: bool = True,
    conn: Optional[str] = None,
    adapter: Optional[str] = None,
    ddl: Optional[bool] = None,
    url: Optional[str] = None,
    table: Optional[Tuple[str, ...]] = None,
    exclude: Optional[Tuple[str, ...]] = None,
    location: Optional[str] = None,
    dry_run: bool = False,
    stats: bool = False,
):
    if color is False:
        os.environ["NO_COLOR"] = "true"

    if conn and url:
        raise click.UsageError("--url and --connection are mutually exclusive options")

    cli_config = CliConfig(
        ddl=ddl,
        tables=list(table) if table else None,
        exclude=list(exclude) if exclude else None,
        url=url,
        location=location,
        adapter=adapter,
        strict=strict,
    )

    configs = load_configs(config)
    config_stack = ConfigStack(cli_config.to_dict(), *configs)

    try:
        root_config = RootConfig.from_stack(config_stack)
    except ConfigError as e:
        raise click.UsageError(*e.args)

    resolver.register_values(
        root_config=root_config,
        verbosity=verbose,
        console=Console(verbosity=verbose),
        connection_name=conn,
        dry_run=bool(dry_run),
        stats=stats if stats is not None else dry_run,
    )


@resolver.command(cli, "backup")
@click.option("--backup-id", default=None, help="Backup manifest id.")
def backup_cli(
    backup_config: BackupConfig,
    backup_db: Session,
    console: Console,
    backup_manifest: Optional[Manifest] = None,
    backup_id: Optional[int] = None,
    stats: bool = False,
    dry_run: bool = False,
):
    """Perform backup."""
    from databudgie.backup import backup_all
    from databudgie.cli.setup import setup

    setup(backup_config.sentry)

    if backup_manifest and backup_id:
        backup_manifest.set_transaction_id(backup_id)

    storage = StorageBackend.from_config(
        backup_config,
        manifest=backup_manifest,
        record_stats=stats,
        perform_writes=not dry_run,
    )

    try:
        backup_all(backup_db, backup_config, storage=storage, console=console)
    except Exception as e:
        console.trace(e)
        raise click.ClickException(str(e))

    if stats:
        storage.print_stats()


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
    console: Console,
    restore_manifest: Optional[Manifest] = None,
    restore_id: Optional[int] = None,
    clean: Optional[bool] = None,
    yes: bool = False,
    stats: bool = False,
    dry_run: bool = False,
):
    """Perform restore."""
    from databudgie.cli.setup import setup
    from databudgie.restore import restore_all

    setup(restore_config.sentry)

    if restore_manifest and restore_id:
        restore_manifest.set_transaction_id(restore_id)

    restore_config.ddl.clean = clean or restore_config.ddl.clean

    if not yes and restore_config.ddl.clean:
        message = "About to delete the database! input 'y' if that's what you want: "
        if input(message) != "y":  # nosec
            return

    if dry_run:
        raise click.UsageError("--dry-run is not (yet) supported for restore")

    storage = StorageBackend.from_config(
        restore_config,
        manifest=restore_manifest,
        record_stats=stats,
        perform_writes=not dry_run,
    )

    try:
        restore_all(restore_db, restore_config=restore_config, storage=storage, console=console)
    except Exception as e:
        console.trace(e)
        raise click.ClickException(*e.args)

    if stats:
        storage.print_stats()


@resolver.command(cli, "config")
def config_cli(root_config: RootConfig):
    """Print dereferenced and populated config."""
    pretty_print(root_config)
