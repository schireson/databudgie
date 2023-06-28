from typing import Iterable, Optional, Tuple

import click
from sqlalchemy.orm import Session

from databudgie import api
from databudgie.cli.base import resolver
from databudgie.cli.config import (
    file_loaders,
    pretty_print,
)
from databudgie.config import (
    BackupConfig,
    ConfigError,
    RestoreConfig,
    RootConfig,
)
from databudgie.manifest.manager import Manifest
from databudgie.output import Console


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
@click.option("-c", "--config", default=None, help="config file", multiple=True)
@click.option(
    "-C",
    "--conn",
    "--connection",
    default=None,
    help="The name of a named connection to use. Takes precedence over the 'url' field if present.",
)
@click.option("-v", "--verbose", count=True, default=0)
@click.option(
    "--ddl/--no-ddl", default=None, is_flag=True, help="Whether to backup the DDL. Overrides the config option, if set"
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
@click.option(
    "--raw-config",
    default=None,
    help="Accepts raw config as a string, rather than searching a file for it.",
)
@click.option(
    "--raw-config-format",
    default="json",
    help="The assumed format of --raw-config. Defaults to `json`. Must be one of: `yml`, `yaml`, `json`, `toml`.",
    type=click.Choice(file_loaders),
)
@click.version_option()
def cli(
    strict: bool = False,
    config: Iterable[str] = (),
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
    raw_config: Optional[str] = None,
    raw_config_format: str = "json",
):
    try:
        root_config = api.root_config(
            strict=strict,
            config=config,
            color=color,
            conn=conn,
            adapter=adapter,
            ddl=ddl,
            url=url,
            table=table,
            exclude=exclude,
            location=location,
            raw_config=raw_config,
            raw_config_format=raw_config_format,
        )
    except ConfigError as e:
        raise click.UsageError(*e.args)

    resolver.register_values(
        root_config=root_config,
        verbosity=verbose,
        console=Console(verbosity=verbose),
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
    try:
        api.backup(
            backup_db,
            backup_config,
            manifest=backup_manifest,
            console=console,
            stats=stats,
            dry_run=dry_run,
            transaction_id=backup_id,
        )
    except Exception as e:
        console.trace(e)
        raise click.ClickException(str(e))


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
    if not yes and (clean or restore_config.ddl.clean):
        message = "About to delete the database! input 'y' if that's what you want: "
        if input(message) != "y":  # nosec
            return

    try:
        api.restore(
            db=restore_db,
            config=restore_config,
            console=console,
            manifest=restore_manifest,
            transaction_id=restore_id,
            clean=clean,
            stats=stats,
            dry_run=dry_run,
        )
    except Exception as e:
        console.trace(e)
        raise click.ClickException(*e.args)


@resolver.command(cli, "config")
def config_cli(root_config: RootConfig):
    """Print dereferenced and populated config."""
    pretty_print(root_config)
