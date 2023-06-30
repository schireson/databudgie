import os
from typing import Iterable, Optional, Tuple

from sqlalchemy.orm import Session

from databudgie.cli.config import CliConfig, collect_config
from databudgie.config import BackupConfig, ConfigError, RestoreConfig
from databudgie.manifest.manager import Manifest
from databudgie.output import Console, default_console
from databudgie.storage import StorageBackend


def root_config(
    strict: bool = False,
    config: Iterable[str] = (),
    color: bool = True,
    conn: Optional[str] = None,
    adapter: Optional[str] = None,
    ddl: Optional[bool] = None,
    url: Optional[str] = None,
    table: Optional[Tuple[str, ...]] = None,
    exclude: Optional[Tuple[str, ...]] = None,
    location: Optional[str] = None,
    raw_config: Optional[str] = None,
    raw_config_format: str = "json",
):
    if color is False:
        os.environ["NO_COLOR"] = "true"

    if conn and url:
        raise ConfigError("--url and --connection are mutually exclusive options")

    cli_config = CliConfig(
        ddl=ddl,
        tables=list(table) if table else None,
        exclude=list(exclude) if exclude else None,
        url=url,
        location=location,
        adapter=adapter,
        strict=strict,
        connection=conn,
    )

    return collect_config(
        cli_config=cli_config,
        file_names=config,
        raw_config=raw_config,
        raw_config_format=raw_config_format,
    )


def backup(
    db: Session,
    config: BackupConfig,
    console: Console = default_console,
    manifest: Optional[Manifest] = None,
    transaction_id: Optional[int] = None,
    stats: bool = False,
    dry_run: bool = False,
):
    """Perform backup."""
    from databudgie.backup import backup_all

    if manifest and transaction_id:
        manifest.set_transaction_id(transaction_id)

    storage = StorageBackend.from_config(
        config,
        manifest=manifest,
        record_stats=stats,
        perform_writes=not dry_run,
    )

    try:
        backup_all(db, config, storage=storage, console=console)
    finally:
        if stats:
            storage.print_stats()


def restore(
    db: Session,
    config: RestoreConfig,
    console: Console,
    manifest: Optional[Manifest] = None,
    transaction_id: Optional[int] = None,
    clean: Optional[bool] = None,
    stats: bool = False,
    dry_run: bool = False,
):
    """Perform restore."""
    from databudgie.restore import restore_all

    if manifest and transaction_id:
        manifest.set_transaction_id(transaction_id)

    config.ddl.clean = clean or config.ddl.clean

    if dry_run:
        raise ConfigError("--dry-run is not (yet) supported for restore")

    storage = StorageBackend.from_config(
        config,
        manifest=manifest,
        record_stats=stats,
        perform_writes=not dry_run,
    )

    try:
        restore_all(db, restore_config=config, storage=storage, console=console)
    finally:
        if stats:
            storage.print_stats()
