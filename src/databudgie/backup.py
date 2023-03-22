from __future__ import annotations

import io
import json
from typing import Sequence

from sqlalchemy.orm import Session

from databudgie.adapter import Adapter
from databudgie.config import BackupConfig, BackupTableConfig
from databudgie.output import Console, default_console, Progress
from databudgie.storage import FileTypes, StorageBackend
from databudgie.table_op import expand_table_ops, TableOp
from databudgie.utils import capture_failures, join_paths


def backup_all(
    session: Session,
    backup_config: BackupConfig,
    storage: StorageBackend | None = None,
    console: Console = default_console,
):
    """Perform backup on all tables in the config.

    Arguments:
        session: A SQLAlchemy session with the PostgreSQL database from which to query data.
        backup_config: config object mapping table names to their query and location.
        storage: Storage backend to use for backing up the data.
        console: Console used for output
    """
    if storage is None:
        storage = StorageBackend.from_config(backup_config)
    adapter = Adapter.get_adapter(session, backup_config.adapter)

    existing_tables = adapter.collect_existing_tables()
    table_ops = expand_table_ops(
        session,
        backup_config.tables,
        existing_tables,
        storage=storage,
        console=console,
        warn_for_unused_tables=True,
    )

    table_ops = adapter.materialize_table_dependencies(
        table_ops,
        console=console,
    )

    backup_ddl(
        backup_config,
        table_ops,
        adapter=adapter,
        storage=storage,
        console=console,
    )
    backup_sequences(
        table_ops,
        adapter=adapter,
        storage=storage,
        console=console,
    )
    backup_tables(
        table_ops=table_ops,
        adapter=adapter,
        storage=storage,
        console=console,
    )


def backup_ddl(
    backup_config: BackupConfig,
    table_ops: list[TableOp[BackupTableConfig]],
    *,
    adapter: Adapter,
    storage: StorageBackend,
    console: Console = default_console,
):
    if not backup_config.ddl.enabled:
        return

    # Backup schemas first
    schema_names = set()
    schemas = []

    for table_op in table_ops:
        schema_op = table_op.schema_op()
        if schema_op.name in schemas:
            continue

        if not table_op.raw_conf.ddl:
            continue

        schema_names.add(schema_op.name)
        schemas.append(schema_op)

    with Progress(console) as progress:
        table_names = []

        total = len(schemas) + len(table_ops)
        task = progress.add_task("Backing up schema DDL", total=total)

        for schema_op in schemas:
            progress.update(task, description=f"Backing up schema DDL: {schema_op.name}")

            buffer = io.BytesIO(adapter.export_schema_ddl(schema_op.name))
            filename = storage.write_buffer(
                join_paths(backup_config.ddl.location, schema_op.location()),
                buffer,
                file_type=FileTypes.ddl,
            )

            console.trace(f"Wrote {schema_op.name} to {filename}")

        console.info("Finished backing up schema DDL")

        for table_op in table_ops:
            if not table_op.raw_conf.ddl:
                continue

            progress.update(task, description=f"Backing up DDL: {table_op.full_name}")
            result = adapter.export_table_ddl(table_op.full_name)

            filename = storage.write_buffer(
                join_paths(backup_config.ddl.location, table_op.location()),
                io.BytesIO(result),
                file_type=FileTypes.ddl,
                name=table_op.full_name,
            )

            console.trace(f"Uploaded {table_op.full_name} to {filename}")
            table_names.append(table_op.full_name)

    console.info("Finished backing up DDL")

    # On the restore-side, the tables may not already exist (at the extreme, you
    # might start with an empty database), so we need to record the set of tables
    # being backed up.
    manifest_data = json.dumps(table_names).encode("utf-8")
    with io.BytesIO(manifest_data) as buffer:
        storage.write_buffer(backup_config.ddl.location, buffer, file_type=FileTypes.manifest)


def backup_sequences(
    table_ops: list[TableOp[BackupTableConfig]],
    *,
    adapter: Adapter,
    storage: StorageBackend,
    console: Console = default_console,
):
    has_sequences = any(o.raw_conf.sequences for o in table_ops)
    if not has_sequences:
        return

    table_sequences = adapter.collect_table_sequences()

    with Progress(console) as progress:
        task = progress.add_task("Backing up sequence positions", total=len(table_ops))

        for table_op in table_ops:
            progress.update(task, description=f"Backing up sequence position: {table_op.full_name}")

            if not table_op.raw_conf.sequences:
                continue

            sequences = table_sequences.get(table_op.full_name)
            if not sequences:
                continue

            sequence_values = {}
            for sequence in sequences:
                sequence_values[sequence] = adapter.collect_sequence_value(sequence)

            result = json.dumps(sequence_values).encode("utf-8")

            path = table_op.location()

            filename = storage.write_buffer(
                join_paths(path, "sequences"),
                io.BytesIO(result),
                file_type=FileTypes.sequences,
                name=table_op.full_name,
            )

            console.trace(f"Wrote {table_op.full_name} sequences to {filename}")

    console.info("Finished backing up sequence positions")


def backup_tables(
    table_ops: Sequence[TableOp],
    storage: StorageBackend,
    *,
    adapter: Adapter,
    console: Console = default_console,
) -> None:
    with Progress(console) as progress:
        task = progress.add_task("Backing up tables", total=len(table_ops))

        for table_op in table_ops:
            progress.update(task, description=f"Backing up table: {table_op.full_name}")

            if not table_op.raw_conf.data:
                continue

            with capture_failures(strict=table_op.raw_conf.strict):
                backup(
                    table_op=table_op,
                    storage=storage,
                    adapter=adapter,
                    console=console,
                )

    console.info("Finished Backing up tables")


def backup(
    *,
    table_op: TableOp[BackupTableConfig],
    adapter: Adapter,
    storage: StorageBackend,
    console: Console = default_console,
):
    """Dump query contents to S3 as a CSV file.

    Arguments:
        config: The raw backup configuration.
        table_op: The table operation being acted up on.
        adapter: the selected behavior adapter
        storage: the storage backend to use for backing up the data.
        console: Console used for output
    """
    buffer = adapter.export_query(table_op.query())

    # path.join will handle optionally trailing slashes in the location
    compression = table_op.raw_conf.compression

    filename = storage.write_buffer(
        table_op.location(), buffer, file_type=FileTypes.data, name=table_op.full_name, compression=compression
    )

    console.trace(f"Uploaded {table_op.full_name} to {filename}")
